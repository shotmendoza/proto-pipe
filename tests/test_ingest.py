"""
Tests for validation_pipeline.ingest

Covers:
- resolve_source (pattern matching)
- _structural_checks (empty file, missing timestamp col)
- ingest_directory (happy path, append, replace, auto-migrate, failures, unmatched)
- ingest_log entries
"""

import pandas as pd
import duckdb

from src.io.ingest import (
    resolve_source,
    _structural_checks,
    init_db,
    _table_exists,
    ingest_directory
)


# ---------------------------------------------------------------------------
# resolve_source
# ---------------------------------------------------------------------------

class TestResolveSource:
    def test_matches_csv_pattern(self, sources_config):
        source = resolve_source("sales_2026-03.csv", sources_config["sources"])
        assert source is not None
        assert source["target_table"] == "sales"

    def test_matches_xlsx_pattern(self, sources_config):
        source = resolve_source("Sales_March.xlsx", sources_config["sources"])
        assert source is not None
        assert source["target_table"] == "sales"

    def test_matches_inventory_pattern(self, sources_config):
        source = resolve_source("inventory_2026-03.csv", sources_config["sources"])
        assert source is not None
        assert source["target_table"] == "inventory"

    def test_returns_none_for_unmatched(self, sources_config):
        assert resolve_source("unknown_data.csv", sources_config["sources"]) is None

    def test_returns_none_for_wrong_extension(self, sources_config):
        assert resolve_source("sales_2026.json", sources_config["sources"]) is None


# ---------------------------------------------------------------------------
# _structural_checks
# ---------------------------------------------------------------------------

class TestStructuralChecks:
    def test_clean_df_passes(self, sales_df):
        source = {"timestamp_col": "updated_at"}
        assert _structural_checks(sales_df, source) == []

    def test_empty_df_fails(self):
        source = {"timestamp_col": "updated_at"}
        issues = _structural_checks(pd.DataFrame(), source)
        assert any("empty" in i.lower() for i in issues)

    def test_missing_timestamp_col_fails(self, sales_df):
        source = {"timestamp_col": "nonexistent_col"}
        issues = _structural_checks(sales_df, source)
        assert any("nonexistent_col" in i for i in issues)

    def test_no_timestamp_col_defined_passes(self, sales_df):
        source = {}
        assert _structural_checks(sales_df, source) == []


# ---------------------------------------------------------------------------
# init_db
# ---------------------------------------------------------------------------

class TestInitDb:
    def test_creates_tables(self, pipeline_db, sources_config):
        init_db(pipeline_db, sources_config["sources"])
        conn = duckdb.connect(pipeline_db)
        assert _table_exists(conn, "sales")
        assert _table_exists(conn, "inventory")
        conn.close()

    def test_creates_ingest_log(self, pipeline_db, sources_config):
        init_db(pipeline_db, sources_config["sources"])
        conn = duckdb.connect(pipeline_db)
        assert _table_exists(conn, "ingest_log")
        conn.close()

    def test_idempotent(self, pipeline_db, sources_config):
        init_db(pipeline_db, sources_config["sources"])
        init_db(pipeline_db, sources_config["sources"])  # should not raise
        conn = duckdb.connect(pipeline_db)
        assert _table_exists(conn, "sales")
        conn.close()


# ---------------------------------------------------------------------------
# ingest_directory — happy path
# ---------------------------------------------------------------------------

class TestIngestDirectoryHappyPath:
    def test_sales_file_ingested(
        self, incoming_dir, sales_csv, pipeline_db, sources_config
    ):
        summary = ingest_directory(
            str(incoming_dir), sources_config["sources"], pipeline_db
        )
        assert summary["sales_2026-03.csv"]["status"] == "ok"
        assert summary["sales_2026-03.csv"]["rows"] == 3

    def test_data_is_queryable(
        self, incoming_dir, sales_csv, pipeline_db, sources_config
    ):
        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)
        conn = duckdb.connect(pipeline_db)
        rows = conn.execute("SELECT * FROM sales").fetchall()
        conn.close()
        assert len(rows) == 3

    def test_multiple_files_ingested(
        self, incoming_dir, sales_csv, inventory_csv, pipeline_db, sources_config
    ):
        summary = ingest_directory(
            str(incoming_dir), sources_config["sources"], pipeline_db
        )
        assert summary["sales_2026-03.csv"]["status"] == "ok"
        assert summary["inventory_2026-03.csv"]["status"] == "ok"

    def test_ingest_log_records_success(
        self, incoming_dir, sales_csv, pipeline_db, sources_config
    ):
        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)
        conn = duckdb.connect(pipeline_db)
        row = conn.execute(
            "SELECT status, rows FROM ingest_log WHERE filename = 'sales_2026-03.csv'"
        ).fetchone()
        conn.close()
        assert row[0] == "ok"
        assert row[1] == 3


# ---------------------------------------------------------------------------
# ingest_directory — append vs replace
# ---------------------------------------------------------------------------

class TestIngestModes:
    def test_append_doubles_rows(
        self, incoming_dir, sales_csv, pipeline_db, sources_config
    ):
        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)
        ingest_directory(
            str(incoming_dir), sources_config["sources"], pipeline_db, mode="append"
        )
        conn = duckdb.connect(pipeline_db)
        count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        conn.close()
        assert count == 6

    def test_replace_keeps_row_count(
        self, incoming_dir, sales_csv, pipeline_db, sources_config
    ):
        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)
        ingest_directory(
            str(incoming_dir), sources_config["sources"], pipeline_db, mode="replace"
        )
        conn = duckdb.connect(pipeline_db)
        count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        conn.close()
        assert count == 3


# ---------------------------------------------------------------------------
# ingest_directory — auto-migration
# ---------------------------------------------------------------------------

class TestAutoMigrate:
    def test_new_column_added_on_second_ingest(
        self, incoming_dir, sales_csv, pipeline_db, sources_config, sales_df
    ):
        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)

        # Write a second file with an extra column
        new_df = sales_df.copy()
        new_df["discount"] = 0.1
        extra_path = incoming_dir / "sales_2026-04.csv"
        new_df.to_csv(extra_path, index=False)

        summary = ingest_directory(
            str(incoming_dir), sources_config["sources"], pipeline_db
        )
        assert "discount" in summary["sales_2026-04.csv"]["new_cols"]

        conn = duckdb.connect(pipeline_db)
        cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'sales'"
            ).fetchall()
        }
        conn.close()
        assert "discount" in cols


# ---------------------------------------------------------------------------
# ingest_directory — failure cases
# ---------------------------------------------------------------------------

class TestIngestFailures:
    def test_empty_file_is_skipped(
        self, incoming_dir, empty_csv, pipeline_db, sources_config
    ):
        summary = ingest_directory(
            str(incoming_dir), sources_config["sources"], pipeline_db
        )
        assert summary["sales_empty.csv"]["status"] == "failed"

    def test_unmatched_file_is_skipped(
        self, incoming_dir, unmatched_csv, pipeline_db, sources_config
    ):
        # unmatched files don't appear in the summary dict but are logged
        summary = ingest_directory(
            str(incoming_dir), sources_config["sources"], pipeline_db
        )
        assert "unknown_data_2026.csv" not in summary

    def test_ingest_log_records_skipped(
        self, incoming_dir, unmatched_csv, pipeline_db, sources_config
    ):
        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)
        conn = duckdb.connect(pipeline_db)
        row = conn.execute(
            "SELECT status FROM ingest_log WHERE filename = 'unknown_data_2026.csv'"
        ).fetchone()
        conn.close()
        assert row[0] == "skipped"

    def test_failed_file_does_not_stop_other_files(
        self, incoming_dir, empty_csv, sales_csv, pipeline_db, sources_config
    ):
        summary = ingest_directory(
            str(incoming_dir), sources_config["sources"], pipeline_db
        )
        assert summary["sales_empty.csv"]["status"] == "failed"
        assert summary["sales_2026-03.csv"]["status"] == "ok"
