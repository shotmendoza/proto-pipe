"""Tests for src.reports.validation_flags

Covers:
- _extract_flagged_rows: mask mode (flag_when=True default, flag_when=False, no pk_col)
- _extract_flagged_rows: mask mode malformed input falls back to summary
- _extract_flagged_rows: check_name appears in mask mode reason
- _extract_flagged_rows: violation_indices mode (backward compat with check_range)
- _extract_flagged_rows: summary fallback (null_check, duplicate_check, schema_check, free-form)
- write_validation_flags: inserts rows, returns count
- write_validation_flags: idempotent on re-run (uuid5 dedup)
- write_validation_flags: different checks same row → two flags
- write_validation_flags: different reports same check+row → two flags
- write_validation_flags: empty list writes nothing
- write_validation_flags: None pk_value still writes (uuid4)
- write_validation_flags: reason truncated to 500 chars
- count_validation_flags: total and scoped by report
- summary_df: groups by report+check, counts correctly, supports report scope
- detail_df: returns one row per flag, supports report scope
- clear_validation_flags: all, scoped to report, scoped to check
- export_validation_report: two sheets present
- export_validation_report: pk_col renamed when all flags share same pk_col
- export_validation_report: generic record_id when pk_cols are mixed
- export_validation_report: scoped export only includes target report
- export_validation_report: raises ValueError when no flags exist
- export_validation_report: creates parent directories
"""

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.reports.validation_flags import (
    _extract_flagged_rows,
    _summarise_result,
    clear_validation_flags,
    count_validation_flags,
    detail_df,
    export_validation_report,
    init_validation_flags_table,
    summary_df,
    write_validation_flags,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def conn(tmp_path):
    """Fresh DuckDB connection with validation_flags table bootstrapped."""
    db_path = str(tmp_path / "test.db")
    c = duckdb.connect(db_path)
    init_validation_flags_table(c)
    yield c
    c.close()


@pytest.fixture()
def sample_df():
    return pd.DataFrame({
        "order_id": ["ORD-001", "ORD-002", "ORD-003"],
        "price":    [100.0,    -5.0,      250.0],
        "region":   ["EMEA",   "APAC",    "EMEA"],
    })


# ---------------------------------------------------------------------------
# _extract_flagged_rows — mask mode
# ---------------------------------------------------------------------------

class TestExtractMaskMode:
    def test_flag_when_true_flags_true_rows(self, sample_df):
        result = {"mask": sample_df["price"] < 0, "flag_when": True}
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="price_check")

        assert len(flags) == 1
        assert flags[0]["pk_value"] == "ORD-002"

    def test_flag_when_omitted_defaults_to_true(self, sample_df):
        result = {"mask": sample_df["price"] < 0}
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="price_check")

        assert len(flags) == 1
        assert flags[0]["pk_value"] == "ORD-002"

    def test_flag_when_false_flags_false_rows(self, sample_df):
        # flag rows where mask is False — i.e. region != "EMEA"
        result = {"mask": sample_df["region"] == "EMEA", "flag_when": False}
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="region_check")

        assert len(flags) == 1
        assert flags[0]["pk_value"] == "ORD-002"

    def test_check_name_included_in_reason(self, sample_df):
        result = {"mask": sample_df["price"] < 0}
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="my_price_check")

        assert "my_price_check" in flags[0]["reason"]

    def test_no_pk_col_gives_none_pk_value(self, sample_df):
        result = {"mask": sample_df["price"] < 0}
        flags = _extract_flagged_rows(result, sample_df, pk_col=None, check_name="c")

        assert len(flags) == 1
        assert flags[0]["pk_value"] is None

    def test_all_rows_pass_mask_returns_empty(self, sample_df):
        # All prices are positive — no rows flagged
        result = {"mask": sample_df["price"] > 1000, "flag_when": True}
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="c")

        assert flags == []

    def test_all_rows_flagged(self, sample_df):
        result = {"mask": sample_df["price"] > -999, "flag_when": True}
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="c")

        assert len(flags) == 3

    def test_malformed_mask_not_a_series_falls_back_to_summary(self, sample_df):
        result = {"mask": "not a series"}
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="c")

        assert len(flags) == 1
        assert flags[0]["pk_value"] is None
        assert "not a pd.Series" in flags[0]["reason"]


# ---------------------------------------------------------------------------
# _extract_flagged_rows — violation_indices mode
# ---------------------------------------------------------------------------

class TestExtractViolationIndicesMode:
    def test_flags_rows_at_given_indices(self, sample_df):
        result = {
            "col": "price", "min_val": 0, "max_val": 500,
            "violations": 1, "violation_indices": [1],
        }
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="range_check")

        assert len(flags) == 1
        assert flags[0]["pk_value"] == "ORD-002"

    def test_reason_includes_col_value_and_bounds(self, sample_df):
        result = {
            "col": "price", "min_val": 0, "max_val": 500,
            "violations": 1, "violation_indices": [1],
        }
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="range_check")

        assert "price" in flags[0]["reason"]
        assert "-5.0" in flags[0]["reason"]
        assert "0" in flags[0]["reason"]
        assert "500" in flags[0]["reason"]

    def test_empty_violation_indices_returns_empty_list(self, sample_df):
        result = {"violations": 0, "violation_indices": []}
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="c")

        assert flags == []

    def test_no_pk_col_gives_none_pk_value(self, sample_df):
        result = {"violation_indices": [0, 2]}
        flags = _extract_flagged_rows(result, sample_df, pk_col=None, check_name="c")

        assert len(flags) == 2
        assert all(f["pk_value"] is None for f in flags)

    def test_multiple_violations_all_flagged(self, sample_df):
        result = {"violation_indices": [0, 1, 2]}
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="c")

        assert len(flags) == 3
        assert {f["pk_value"] for f in flags} == {"ORD-001", "ORD-002", "ORD-003"}


# ---------------------------------------------------------------------------
# _extract_flagged_rows — summary fallback mode
# ---------------------------------------------------------------------------

class TestExtractSummaryMode:
    def test_null_check_result_one_summary_flag(self, sample_df):
        result = {"null_counts": {"price": 0, "region": 2}, "has_nulls": True}
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="null_check")

        assert len(flags) == 1
        assert flags[0]["pk_value"] is None
        assert "null" in flags[0]["reason"].lower()

    def test_duplicate_check_result_one_summary_flag(self, sample_df):
        result = {"duplicate_count": 3, "has_duplicates": True, "subset": None}
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="dup_check")

        assert len(flags) == 1
        assert "3" in flags[0]["reason"]

    def test_schema_check_result_one_summary_flag(self, sample_df):
        result = {"missing_cols": ["foo", "bar"], "extra_cols": [], "matches": False}
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="schema_check")

        assert len(flags) == 1
        assert "missing" in flags[0]["reason"].lower()

    def test_free_form_result_one_summary_flag(self, sample_df):
        result = {"my_custom_key": 42, "other_key": "value"}
        flags = _extract_flagged_rows(result, sample_df, pk_col="order_id", check_name="custom_check")

        assert len(flags) == 1
        assert flags[0]["pk_value"] is None

    def test_empty_result_dict_one_summary_flag(self, sample_df):
        flags = _extract_flagged_rows({}, sample_df, pk_col="order_id", check_name="c")

        assert len(flags) == 1
        assert flags[0]["pk_value"] is None


# ---------------------------------------------------------------------------
# write_validation_flags
# ---------------------------------------------------------------------------

class TestWriteValidationFlags:
    def test_inserts_and_returns_count(self, conn):
        flags = [
            {"pk_value": "ORD-001", "reason": "price out of range"},
            {"pk_value": "ORD-002", "reason": "price out of range"},
        ]
        count = write_validation_flags(conn, "sales_report", "range_check", "sales", "order_id", flags)

        assert count == 2
        assert count_validation_flags(conn) == 2

    def test_idempotent_same_report_check_pk(self, conn):
        flags = [{"pk_value": "ORD-001", "reason": "price out of range"}]
        write_validation_flags(conn, "r", "range_check", "sales", "order_id", flags)
        write_validation_flags(conn, "r", "range_check", "sales", "order_id", flags)

        # uuid5 dedup: same (report, check, pk_value) → same id → ON CONFLICT DO NOTHING
        assert count_validation_flags(conn) == 1

    def test_different_checks_same_row_produces_two_flags(self, conn):
        flag = [{"pk_value": "ORD-001", "reason": "bad"}]
        write_validation_flags(conn, "r", "check_a", "sales", "order_id", flag)
        write_validation_flags(conn, "r", "check_b", "sales", "order_id", flag)

        assert count_validation_flags(conn) == 2

    def test_different_reports_same_check_and_row_produces_two_flags(self, conn):
        flag = [{"pk_value": "ORD-001", "reason": "bad"}]
        write_validation_flags(conn, "report_a", "range_check", "sales", "order_id", flag)
        write_validation_flags(conn, "report_b", "range_check", "sales", "order_id", flag)

        assert count_validation_flags(conn) == 2

    def test_empty_list_writes_nothing(self, conn):
        count = write_validation_flags(conn, "r", "c", "t", "id", [])

        assert count == 0
        assert count_validation_flags(conn) == 0

    def test_none_pk_value_still_writes_flag(self, conn):
        # Summary flags (no row id) use uuid4 — non-deduplicable but still written
        flags = [{"pk_value": None, "reason": "2 null values in region"}]
        count = write_validation_flags(conn, "r", "null_check", "t", None, flags)

        assert count == 1
        assert count_validation_flags(conn) == 1

    def test_reason_truncated_to_500_chars(self, conn):
        flags = [{"pk_value": "X", "reason": "a" * 600}]
        write_validation_flags(conn, "r", "c", "t", "id", flags)

        det = detail_df(conn)
        assert len(det.iloc[0]["reason"]) == 500


# ---------------------------------------------------------------------------
# count_validation_flags / summary_df / detail_df / clear_validation_flags
# ---------------------------------------------------------------------------

class TestQueryHelpers:
    def test_count_returns_total_across_all_reports(self, conn):
        flags = [{"pk_value": f"ORD-00{i}", "reason": "bad"} for i in range(4)]
        write_validation_flags(conn, "r", "c", "t", "order_id", flags)

        assert count_validation_flags(conn) == 4

    def test_count_scoped_by_report_name(self, conn):
        write_validation_flags(conn, "report_a", "c", "t", "id",
                               [{"pk_value": "X", "reason": "r"}])
        write_validation_flags(conn, "report_b", "c", "t", "id",
                               [{"pk_value": "Y", "reason": "r"}])

        assert count_validation_flags(conn, "report_a") == 1
        assert count_validation_flags(conn, "report_b") == 1
        assert count_validation_flags(conn) == 2

    def test_summary_df_groups_by_report_and_check(self, conn):
        write_validation_flags(conn, "sales_report", "range_check", "sales", "order_id",
                               [{"pk_value": "ORD-001", "reason": "x"},
                                {"pk_value": "ORD-002", "reason": "x"}])
        write_validation_flags(conn, "sales_report", "null_check", "sales", "order_id",
                               [{"pk_value": "ORD-003", "reason": "y"}])

        summ = summary_df(conn)
        assert len(summ) == 2
        assert summ[summ["check_name"] == "range_check"].iloc[0]["flagged_count"] == 2
        assert summ[summ["check_name"] == "null_check"].iloc[0]["flagged_count"] == 1

    def test_summary_df_scoped_to_one_report(self, conn):
        write_validation_flags(conn, "report_a", "c", "t", "id",
                               [{"pk_value": "X", "reason": "r"}])
        write_validation_flags(conn, "report_b", "c", "t", "id",
                               [{"pk_value": "Y", "reason": "r"}])

        summ = summary_df(conn, "report_a")
        assert len(summ) == 1
        assert summ.iloc[0]["report_name"] == "report_a"

    def test_detail_df_returns_one_row_per_flag(self, conn):
        flags = [{"pk_value": f"ORD-00{i}", "reason": "bad"} for i in range(5)]
        write_validation_flags(conn, "r", "c", "t", "order_id", flags)

        assert len(detail_df(conn)) == 5

    def test_detail_df_scoped_to_one_report(self, conn):
        write_validation_flags(conn, "report_a", "c", "t", "id",
                               [{"pk_value": "X", "reason": "r"}])
        write_validation_flags(conn, "report_b", "c", "t", "id",
                               [{"pk_value": "Y", "reason": "r"},
                                {"pk_value": "Z", "reason": "r"}])

        det = detail_df(conn, "report_b")
        assert len(det) == 2
        assert set(det["pk_value"]) == {"Y", "Z"}

    def test_clear_all_flags(self, conn):
        write_validation_flags(conn, "r", "c", "t", "id",
                               [{"pk_value": "X", "reason": "r"},
                                {"pk_value": "Y", "reason": "r"}])
        cleared = clear_validation_flags(conn)

        assert cleared == 2
        assert count_validation_flags(conn) == 0

    def test_clear_scoped_to_report_leaves_others(self, conn):
        write_validation_flags(conn, "report_a", "c", "t", "id",
                               [{"pk_value": "X", "reason": "r"}])
        write_validation_flags(conn, "report_b", "c", "t", "id",
                               [{"pk_value": "Y", "reason": "r"}])
        clear_validation_flags(conn, report_name="report_a")

        assert count_validation_flags(conn, "report_a") == 0
        assert count_validation_flags(conn, "report_b") == 1

    def test_clear_scoped_to_check_leaves_others(self, conn):
        write_validation_flags(conn, "r", "check_a", "t", "id",
                               [{"pk_value": "X", "reason": "r"}])
        write_validation_flags(conn, "r", "check_b", "t", "id",
                               [{"pk_value": "Y", "reason": "r"}])
        clear_validation_flags(conn, check_name="check_a")

        assert count_validation_flags(conn) == 1
        assert detail_df(conn).iloc[0]["check_name"] == "check_b"


# ---------------------------------------------------------------------------
# export_validation_report
# ---------------------------------------------------------------------------

class TestExportValidationReport:
    def test_produces_detail_and_summary_sheets(self, conn, tmp_path):
        write_validation_flags(conn, "sales_report", "range_check", "sales", "order_id",
                               [{"pk_value": "ORD-002", "reason": "price negative"}])

        out_path = str(tmp_path / "validation.xlsx")
        detail_count, summary_count = export_validation_report(conn, out_path, "sales_report")

        assert Path(out_path).exists()
        assert detail_count == 1
        assert summary_count == 1
        wb = pd.ExcelFile(out_path)
        assert "Detail" in wb.sheet_names
        assert "Summary" in wb.sheet_names

    def test_detail_renames_pk_col_when_all_flags_share_same_pk_col(self, conn, tmp_path):
        write_validation_flags(conn, "r", "c", "t", "order_id",
                               [{"pk_value": "ORD-001", "reason": "bad"}])

        out_path = str(tmp_path / "v.xlsx")
        export_validation_report(conn, out_path)

        det = pd.read_excel(out_path, sheet_name="Detail")
        assert "order_id" in det.columns
        assert "pk_value" not in det.columns

    def test_detail_uses_record_id_when_pk_cols_are_mixed(self, conn, tmp_path):
        write_validation_flags(conn, "r", "c1", "t1", "order_id",
                               [{"pk_value": "A", "reason": "x"}])
        write_validation_flags(conn, "r", "c2", "t2", "internal_id",
                               [{"pk_value": "B", "reason": "y"}])

        out_path = str(tmp_path / "v.xlsx")
        export_validation_report(conn, out_path)

        det = pd.read_excel(out_path, sheet_name="Detail")
        assert "record_id" in det.columns
        assert "pk_value" not in det.columns

    def test_scoped_export_only_includes_target_report(self, conn, tmp_path):
        write_validation_flags(conn, "report_a", "c", "t", "id",
                               [{"pk_value": "X", "reason": "r"}])
        write_validation_flags(conn, "report_b", "c", "t", "id",
                               [{"pk_value": "Y", "reason": "r"},
                                {"pk_value": "Z", "reason": "r"}])

        out_path = str(tmp_path / "v.xlsx")
        detail_count, _ = export_validation_report(conn, out_path, "report_b")

        assert detail_count == 2

    def test_raises_when_no_flags_for_scope(self, conn, tmp_path):
        out_path = str(tmp_path / "empty.xlsx")
        with pytest.raises(ValueError, match="No validation flags"):
            export_validation_report(conn, out_path, "nonexistent_report")

    def test_creates_parent_directories_if_missing(self, conn, tmp_path):
        write_validation_flags(conn, "r", "c", "t", "id",
                               [{"pk_value": "X", "reason": "r"}])

        out_path = str(tmp_path / "nested" / "deep" / "v.xlsx")
        export_validation_report(conn, out_path)

        assert Path(out_path).exists()