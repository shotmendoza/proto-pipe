"""Tests for bulk_upsert_validation_pass behavioral guarantees.

CLAUDE.md alignment:
- Follows the same pattern as bulk flag writes in ingest: accumulate in memory,
  write once in the sequential write phase
- ON CONFLICT (pk_value, table_name, report_name) updates all mutable fields
- Empty entries list is a no-op — no query issued
- All entries for one report share the same table_name and report_name
- Produces identical state to N individual upsert_validation_pass calls

In-code fake data, no static files. Tests use pipeline_db and watermark_store
fixtures from conftest.py.
"""
from __future__ import annotations

import duckdb
import pytest

from proto_pipe.io.db import (
    init_all_pipeline_tables,
    bulk_upsert_validation_pass,
    upsert_validation_pass,
)


def _init(pipeline_db: str) -> None:
    with duckdb.connect(pipeline_db) as conn:
        init_all_pipeline_tables(conn)


def _read_pass(conn: duckdb.DuckDBPyConnection, table_name: str, report_name: str) -> list[dict]:
    rows = conn.execute(
        "SELECT pk_value, row_hash, check_set_hash, status "
        "FROM validation_pass "
        "WHERE table_name = ? AND report_name = ? "
        "ORDER BY pk_value",
        [table_name, report_name],
    ).fetchall()
    return [
        {"pk_value": r[0], "row_hash": r[1], "check_set_hash": r[2], "status": r[3]}
        for r in rows
    ]


def _make_entries(n: int, status: str = "passed") -> list[dict]:
    return [
        {
            "pk_value": f"PK-{i:06d}",
            "row_hash": f"hash_{i}",
            "check_set_hash": "checkset_abc",
            "status": status,
        }
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Core guarantees
# ---------------------------------------------------------------------------

class TestBulkUpsertValidationPass:
    """Behavioral guarantees for bulk_upsert_validation_pass.

    Guarantee 1: All entries written in one call — row count matches input
    Guarantee 2: ON CONFLICT updates all mutable fields
    Guarantee 3: Empty entries list is a no-op
    Guarantee 4: Produces identical state to N individual upserts
    Guarantee 5: table_name and report_name correctly scoped per report
    """

    def test_all_entries_written(self, pipeline_db):
        """All entries in the list are inserted in one call."""
        _init(pipeline_db)
        entries = _make_entries(10)

        with duckdb.connect(pipeline_db) as conn:
            bulk_upsert_validation_pass(conn, "sales", "sales_report", entries)
            rows = _read_pass(conn, "sales", "sales_report")

        assert len(rows) == 10
        assert {r["pk_value"] for r in rows} == {e["pk_value"] for e in entries}

    def test_on_conflict_updates_row_hash(self, pipeline_db):
        """ON CONFLICT: row_hash is updated when the same pk is re-upserted."""
        _init(pipeline_db)
        entries = _make_entries(3)

        with duckdb.connect(pipeline_db) as conn:
            bulk_upsert_validation_pass(conn, "sales", "sales_report", entries)

            # Second upsert with different hashes
            updated = [
                {**e, "row_hash": f"new_hash_{i}"}
                for i, e in enumerate(entries)
            ]
            bulk_upsert_validation_pass(conn, "sales", "sales_report", updated)

            rows = _read_pass(conn, "sales", "sales_report")

        assert len(rows) == 3, "No duplicate rows — conflict resolved via UPDATE"
        for row in rows:
            assert row["row_hash"].startswith("new_hash_"), (
                "row_hash must be updated on conflict"
            )

    def test_on_conflict_updates_status(self, pipeline_db):
        """ON CONFLICT: status is updated from 'failed' to 'passed'."""
        _init(pipeline_db)
        entries = _make_entries(3, status="failed")

        with duckdb.connect(pipeline_db) as conn:
            bulk_upsert_validation_pass(conn, "sales", "sales_report", entries)

            corrected = [{**e, "status": "passed"} for e in entries]
            bulk_upsert_validation_pass(conn, "sales", "sales_report", corrected)

            rows = _read_pass(conn, "sales", "sales_report")

        assert all(r["status"] == "passed" for r in rows), (
            "status must update to 'passed' on conflict"
        )

    def test_on_conflict_updates_check_set_hash(self, pipeline_db):
        """ON CONFLICT: check_set_hash is updated when checks change."""
        _init(pipeline_db)
        entries = _make_entries(3)

        with duckdb.connect(pipeline_db) as conn:
            bulk_upsert_validation_pass(conn, "sales", "sales_report", entries)

            new_checkset = [{**e, "check_set_hash": "new_checkset_xyz"} for e in entries]
            bulk_upsert_validation_pass(conn, "sales", "sales_report", new_checkset)

            rows = _read_pass(conn, "sales", "sales_report")

        assert all(r["check_set_hash"] == "new_checkset_xyz" for r in rows)

    def test_empty_entries_is_noop(self, pipeline_db):
        """Empty entries list issues no query and leaves table unchanged."""
        _init(pipeline_db)

        with duckdb.connect(pipeline_db) as conn:
            bulk_upsert_validation_pass(conn, "sales", "sales_report", [])
            count = conn.execute(
                "SELECT count(*) FROM validation_pass"
            ).fetchone()[0]

        assert count == 0, "Empty entries must not insert any rows"

    def test_produces_identical_state_to_individual_upserts(self, pipeline_db, tmp_path):
        """Bulk result matches N individual upsert_validation_pass calls exactly."""
        entries = _make_entries(50)

        # DB A: individual upserts
        db_a = str(tmp_path / "a.db")
        with duckdb.connect(db_a) as conn:
            init_all_pipeline_tables(conn)
            for e in entries:
                upsert_validation_pass(
                    conn, "sales", "sales_report",
                    e["pk_value"], e["row_hash"], e["check_set_hash"], e["status"],
                )
            rows_a = _read_pass(conn, "sales", "sales_report")

        # DB B: bulk upsert
        db_b = str(tmp_path / "b.db")
        with duckdb.connect(db_b) as conn:
            init_all_pipeline_tables(conn)
            bulk_upsert_validation_pass(conn, "sales", "sales_report", entries)
            rows_b = _read_pass(conn, "sales", "sales_report")

        assert rows_a == rows_b, (
            "bulk_upsert_validation_pass must produce identical state to "
            "N individual upsert_validation_pass calls"
        )

    def test_scoped_to_report_table_name(self, pipeline_db):
        """Entries are scoped to (table_name, report_name) — no cross-report bleed."""
        _init(pipeline_db)
        entries = _make_entries(5)

        with duckdb.connect(pipeline_db) as conn:
            bulk_upsert_validation_pass(conn, "sales", "sales_report", entries)
            bulk_upsert_validation_pass(conn, "inventory", "inventory_report", entries)

            sales_rows = _read_pass(conn, "sales", "sales_report")
            inv_rows = _read_pass(conn, "inventory", "inventory_report")
            total = conn.execute("SELECT count(*) FROM validation_pass").fetchone()[0]

        assert len(sales_rows) == 5
        assert len(inv_rows) == 5
        assert total == 10, "Each (table_name, report_name) pair is independent"

    def test_large_batch_completes(self, pipeline_db):
        """Large batch (10k entries) completes without error — no per-row overhead."""
        _init(pipeline_db)
        entries = _make_entries(10_000)

        with duckdb.connect(pipeline_db) as conn:
            bulk_upsert_validation_pass(conn, "sales", "sales_report", entries)
            count = conn.execute(
                "SELECT count(*) FROM validation_pass "
                "WHERE table_name = 'sales' AND report_name = 'sales_report'"
            ).fetchone()[0]

        assert count == 10_000
