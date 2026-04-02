"""Tests for validation_block — the new validation failure table.

Replaces the old test_validation_flags.py which tested
proto_pipe.reports.validation_flags (now deleted).

Covers:
- write_validation_flags (flagging.py): inserts, idempotency, count
- Composite key uniqueness: same pk, different checks → two flags
- Different reports, same check+pk → two flags
- None pk_value still writes (uuid4)
- Reason truncated to 500 chars
- Direct validation_block queries: count, detail, summary
- build_validation_flag_export: enriched view joined to report table
- export-validation CLI helper inline (openpyxl two-sheet output)
"""

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from proto_pipe.io.db import init_all_pipeline_tables, flag_id_for
from proto_pipe.pipelines.flagging import FlagRecord, write_validation_flags, build_validation_flag_export


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def conn(tmp_path):
    """Fresh DuckDB connection with all pipeline tables bootstrapped."""
    c = duckdb.connect(str(tmp_path / "test.db"))
    init_all_pipeline_tables(c)
    yield c
    c.close()


@pytest.fixture()
def report_table(conn):
    """Seed a simple report table for join tests."""
    conn.execute("""
        CREATE TABLE sales_report (
            order_id VARCHAR,
            price    DOUBLE,
            region   VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO sales_report VALUES
            ('ORD-001', 100.0, 'EMEA'),
            ('ORD-002', -5.0,  'APAC'),
            ('ORD-003', 250.0, 'EMEA')
    """)
    return "sales_report"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flag(conn, report_name, check_name, pk_values, table_name="sales_report"):
    """Write validation flags for given pk_values. Returns count written."""
    flags = [
        FlagRecord(
            id="placeholder",  # recomputed by write_validation_flags
            table_name=table_name,
            report_name=report_name,
            check_name=check_name,
            pk_value=pk,
            reason=f"check '{check_name}' failed for {pk}",
        )
        for pk in pk_values
    ]
    return write_validation_flags(conn, flags)


def _count(conn, report_name=None):
    q = "SELECT count(*) FROM validation_block"
    p = []
    if report_name:
        q += " WHERE report_name = ?"
        p.append(report_name)
    return conn.execute(q, p).fetchone()[0]


def _detail(conn, report_name=None):
    q = "SELECT * FROM validation_block"
    p = []
    if report_name:
        q += " WHERE report_name = ?"
        p.append(report_name)
    q += " ORDER BY flagged_at"
    return conn.execute(q, p).df()


def _summary(conn, report_name=None):
    q = """
        SELECT report_name, check_name, count(*) AS flagged_count
        FROM validation_block
        {}
        GROUP BY report_name, check_name
        ORDER BY report_name, check_name
    """.format("WHERE report_name = ?" if report_name else "")
    p = [report_name] if report_name else []
    return conn.execute(q, p).df()


# ---------------------------------------------------------------------------
# write_validation_flags
# ---------------------------------------------------------------------------

class TestWriteValidationFlags:
    def test_inserts_and_returns_count(self, conn):
        count = _flag(conn, "r", "range_check", ["ORD-001", "ORD-002"])
        assert count == 2
        assert _count(conn) == 2

    def test_idempotent_same_report_check_pk(self, conn):
        _flag(conn, "r", "range_check", ["ORD-001"])
        _flag(conn, "r", "range_check", ["ORD-001"])
        # Same (report, check, pk) → same id → ON CONFLICT DO NOTHING
        assert _count(conn) == 1

    def test_different_checks_same_row_produces_two_flags(self, conn):
        _flag(conn, "r", "check_a", ["ORD-001"])
        _flag(conn, "r", "check_b", ["ORD-001"])
        assert _count(conn) == 2

    def test_different_reports_same_check_and_row_produces_two_flags(self, conn):
        _flag(conn, "report_a", "range_check", ["ORD-001"])
        _flag(conn, "report_b", "range_check", ["ORD-001"])
        assert _count(conn) == 2

    def test_empty_list_writes_nothing(self, conn):
        count = write_validation_flags(conn, [])
        assert count == 0
        assert _count(conn) == 0

    def test_none_pk_value_still_writes_flag(self, conn):
        flags = [FlagRecord(
            id="placeholder",
            table_name="t",
            report_name="r",
            check_name="null_check",
            pk_value=None,
            reason="summary failure",
        )]
        count = write_validation_flags(conn, flags)
        assert count == 1
        assert _count(conn) == 1

    def test_reason_truncated_to_500_chars(self, conn):
        flags = [FlagRecord(
            id="placeholder",
            table_name="t",
            report_name="r",
            check_name="c",
            pk_value="X",
            reason="a" * 600,
        )]
        write_validation_flags(conn, flags)
        det = _detail(conn)
        assert len(det.iloc[0]["reason"]) == 500


# ---------------------------------------------------------------------------
# Count / summary / detail queries
# ---------------------------------------------------------------------------

class TestQueryHelpers:
    def test_count_total_across_all_reports(self, conn):
        _flag(conn, "r", "c", ["ORD-001", "ORD-002", "ORD-003", "ORD-004"])
        assert _count(conn) == 4

    def test_count_scoped_by_report(self, conn):
        _flag(conn, "report_a", "c", ["X"])
        _flag(conn, "report_b", "c", ["Y"])
        assert _count(conn, "report_a") == 1
        assert _count(conn, "report_b") == 1
        assert _count(conn) == 2

    def test_summary_groups_by_report_and_check(self, conn):
        _flag(conn, "sales_report", "range_check", ["ORD-001", "ORD-002"])
        _flag(conn, "sales_report", "null_check", ["ORD-003"])
        summ = _summary(conn)
        assert len(summ) == 2
        rc = summ[summ["check_name"] == "range_check"].iloc[0]
        assert rc["flagged_count"] == 2

    def test_summary_scoped_to_one_report(self, conn):
        _flag(conn, "report_a", "c", ["X"])
        _flag(conn, "report_b", "c", ["Y"])
        summ = _summary(conn, "report_a")
        assert len(summ) == 1
        assert summ.iloc[0]["report_name"] == "report_a"

    def test_detail_returns_one_row_per_flag(self, conn):
        _flag(conn, "r", "c", [f"ORD-00{i}" for i in range(5)])
        assert len(_detail(conn)) == 5

    def test_detail_scoped_to_one_report(self, conn):
        _flag(conn, "report_a", "c", ["X"])
        _flag(conn, "report_b", "c", ["Y", "Z"])
        det = _detail(conn, "report_b")
        assert len(det) == 2
        assert set(det["pk_value"]) == {"Y", "Z"}

    def test_clear_all_flags(self, conn):
        _flag(conn, "r", "c", ["X", "Y"])
        conn.execute("DELETE FROM validation_block")
        assert _count(conn) == 0

    def test_clear_scoped_to_report(self, conn):
        _flag(conn, "report_a", "c", ["X"])
        _flag(conn, "report_b", "c", ["Y"])
        conn.execute("DELETE FROM validation_block WHERE report_name = 'report_a'")
        assert _count(conn, "report_a") == 0
        assert _count(conn, "report_b") == 1

    def test_clear_scoped_to_check(self, conn):
        _flag(conn, "r", "check_a", ["X"])
        _flag(conn, "r", "check_b", ["Y"])
        conn.execute("DELETE FROM validation_block WHERE check_name = 'check_a'")
        assert _count(conn) == 1
        assert _detail(conn).iloc[0]["check_name"] == "check_b"


# ---------------------------------------------------------------------------
# build_validation_flag_export — enriched view joined to report table
# ---------------------------------------------------------------------------

class TestBuildValidationFlagExport:
    def test_returns_report_columns_plus_flag_columns(self, conn, report_table):
        _flag(conn, "sales_validation", "range_check", ["ORD-002"], table_name=report_table)
        df = build_validation_flag_export(conn, report_table, "sales_validation", "order_id")
        assert "_flag_id" in df.columns
        assert "_flag_reason" in df.columns
        assert "order_id" in df.columns
        assert "price" in df.columns

    def test_only_flagged_rows_returned(self, conn, report_table):
        _flag(conn, "sales_validation", "range_check", ["ORD-002"], table_name=report_table)
        df = build_validation_flag_export(conn, report_table, "sales_validation", "order_id")
        assert len(df) == 1
        assert df["order_id"].iloc[0] == "ORD-002"

    def test_multiple_failures_all_returned(self, conn, report_table):
        _flag(conn, "r", "c", ["ORD-001", "ORD-003"], table_name=report_table)
        df = build_validation_flag_export(conn, report_table, "r", "order_id")
        assert len(df) == 2

    def test_empty_when_no_flags(self, conn, report_table):
        df = build_validation_flag_export(conn, report_table, "no_flags", "order_id")
        assert df.empty

    def test_flag_id_is_deterministic(self, conn, report_table):
        """Same (report, check, pk) always produces the same _flag_id."""
        _flag(conn, "r", "range_check", ["ORD-002"], table_name=report_table)
        df1 = build_validation_flag_export(conn, report_table, "r", "order_id")
        expected_id = hashlib.md5("r:range_check:ORD-002".encode()).hexdigest()
        assert df1["_flag_id"].iloc[0] == expected_id


# ---------------------------------------------------------------------------
# Excel export (inline — no CLI dependency)
# ---------------------------------------------------------------------------

class TestExcelExport:
    def test_produces_two_sheet_excel(self, conn, report_table, tmp_path):
        _flag(conn, "sales_validation", "range_check", ["ORD-002"], table_name=report_table)
        df = build_validation_flag_export(conn, report_table, "sales_validation", "order_id")
        summary = _summary(conn, "sales_validation")

        out = tmp_path / "validation.xlsx"
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Detail", index=False)
            summary.to_excel(writer, sheet_name="Summary", index=False)

        assert out.exists()
        xl = pd.ExcelFile(out)
        assert "Detail" in xl.sheet_names
        assert "Summary" in xl.sheet_names

    def test_detail_sheet_has_flag_id(self, conn, report_table, tmp_path):
        _flag(conn, "r", "c", ["ORD-001"], table_name=report_table)
        df = build_validation_flag_export(conn, report_table, "r", "order_id")

        out = tmp_path / "v.xlsx"
        df.to_excel(out, sheet_name="Detail", index=False)
        det = pd.read_excel(out, sheet_name="Detail")
        assert "_flag_id" in det.columns

    def test_raises_when_no_flags(self, conn, report_table, tmp_path):
        df = build_validation_flag_export(conn, report_table, "no_flags", "order_id")
        assert df.empty