"""Tests for validation_pipeline.ingest

Covers:
- resolve_source (pattern matching)
- _structural_checks (empty file, missing timestamp col)
- ingest_directory (happy path, append, replace, auto-migrate, failures, unmatched)
- ingest_log entries
"""

import duckdb
import pandas as pd
import pytest

from proto_pipe.io.db import table_exists
from proto_pipe.io.ingest import (
    resolve_source,
    init_db,
    ingest_directory,
    ingest_single_file,
    init_source_tables,
)
from proto_pipe.io.db import init_all_pipeline_tables


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
# Structural checks — tested via ingest_single_file
# _structural_checks was removed; checks now run from file header inside
# ingest_single_file. Tests use actual CSV files and check result status.
# ---------------------------------------------------------------------------

@pytest.fixture()
def _conn(tmp_path):
    """Fresh DB connection with all pipeline tables."""
    import duckdb
    conn = duckdb.connect(str(tmp_path / "struct_test.db"))
    init_all_pipeline_tables(conn)
    yield conn
    conn.close()


def _source(timestamp_col="updated_at", primary_key=None):
    s = {"name": "sales", "target_table": "sales", "patterns": ["sales_*.csv"]}
    if timestamp_col:
        s["timestamp_col"] = timestamp_col
    if primary_key:
        s["primary_key"] = primary_key
    return s


class TestStructuralChecks:
    def test_empty_file_fails(self, tmp_path, _conn):
        path = tmp_path / "sales_empty.csv"
        path.write_text("order_id,price,updated_at\n")
        result = ingest_single_file(_conn, path, _source())
        assert result["status"] == "failed"
        assert "empty" in result["message"].lower()

    def test_missing_timestamp_col_fails(self, tmp_path, _conn, sales_df):
        path = tmp_path / "sales_2026.csv"
        sales_df.to_csv(path, index=False)
        result = ingest_single_file(_conn, path, _source(timestamp_col="nonexistent_col"))
        assert result["status"] == "failed"
        assert "nonexistent_col" in result["message"]

    def test_clean_file_passes(self, tmp_path, _conn, sales_df):
        path = tmp_path / "sales_2026.csv"
        sales_df.to_csv(path, index=False)
        result = ingest_single_file(_conn, path, _source())
        assert result["status"] == "ok"

    def test_no_timestamp_col_defined_passes(self, tmp_path, _conn, sales_df):
        path = tmp_path / "sales_2026.csv"
        sales_df.to_csv(path, index=False)
        result = ingest_single_file(_conn, path, _source(timestamp_col=None))
        assert result["status"] == "ok"

    def test_null_primary_key_fails(self, tmp_path, _conn, sales_df):
        df = sales_df.copy()
        df.loc[0, "order_id"] = None
        path = tmp_path / "sales_2026.csv"
        df.to_csv(path, index=False)
        result = ingest_single_file(_conn, path, _source(primary_key="order_id"))
        assert result["status"] == "failed"
        assert "NULL" in result["message"] or "null" in result["message"].lower()

    def test_null_primary_key_message_includes_count(self, tmp_path, _conn, sales_df):
        df = sales_df.copy()
        df.loc[0, "order_id"] = None
        df.loc[2, "order_id"] = None
        path = tmp_path / "sales_2026.csv"
        df.to_csv(path, index=False)
        result = ingest_single_file(_conn, path, _source(primary_key="order_id"))
        assert result["status"] == "failed"
        assert "2" in result["message"]


# ---------------------------------------------------------------------------
# init_db
# ---------------------------------------------------------------------------

class TestInitDb:
    def test_creates_tables(self, pipeline_db, sources_config):
        init_db(pipeline_db)
        init_source_tables(pipeline_db, sources_config["sources"])
        conn = duckdb.connect(pipeline_db)
        assert table_exists(conn, "sales")
        assert table_exists(conn, "inventory")
        conn.close()

    def test_creates_ingest_state(self, pipeline_db, sources_config):
        init_db(pipeline_db)
        init_source_tables(pipeline_db, sources_config["sources"])
        conn = duckdb.connect(pipeline_db)
        assert table_exists(conn, "ingest_state")
        conn.close()

    def test_idempotent(self, pipeline_db, sources_config):
        init_db(pipeline_db)
        init_source_tables(pipeline_db, sources_config["sources"])
        init_db(pipeline_db)
        init_source_tables(pipeline_db, sources_config["sources"])  # should not raise
        conn = duckdb.connect(pipeline_db)
        assert table_exists(conn, "sales")
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
            "SELECT status, rows FROM ingest_state WHERE filename = 'sales_2026-03.csv'"
        ).fetchone()
        conn.close()
        assert row[0] == "ok"
        assert row[1] == 3


# ---------------------------------------------------------------------------
# ingest_directory — append vs replace
# ---------------------------------------------------------------------------

class TestIngestModes:

    def test_append_doubles_rows(
            self,
            incoming_dir,
            sales_csv,
            pipeline_db,
            sources_config
    ):
        # First ingest — loads sales_2026-03.csv (3 rows)
        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)

        # Drop a second file with a different name so deduplication doesn't skip it
        sales_csv2 = incoming_dir / "sales_2026-04.csv"
        pd.DataFrame(
            [
                {"order_id": 4, "price": 40.0, "updated_at": "2026-04-01"},
                {"order_id": 5, "price": 50.0, "updated_at": "2026-04-02"},
                {"order_id": 6, "price": 60.0, "updated_at": "2026-04-03"},
            ]
        ).to_csv(sales_csv2, index=False)

        # Second ingest — loads sales_2026-04.csv, skips sales_2026-03.csv
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
            "SELECT status FROM ingest_state WHERE filename = 'unknown_data_2026.csv'"
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


    # Add these two methods to the existing TestIngestDirectory class in test_ingest.py

    def test_ingested_at_column_added(self, pipeline_db, incoming_dir, sources_config, sales_df):
        """_ingested_at column is added to every ingested row."""
        path = incoming_dir / "sales_2026-03.csv"
        sales_df.to_csv(path, index=False)

        init_db(pipeline_db)
        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)

        conn = duckdb.connect(pipeline_db)
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'sales'"
        ).df()["column_name"].tolist()
        conn.close()
        assert "_ingested_at" in cols


    def test_ingested_at_not_null(self, pipeline_db, incoming_dir, sources_config, sales_df):
        """_ingested_at is non-null for all ingested rows."""
        path = incoming_dir / "sales_2026-03.csv"
        sales_df.to_csv(path, index=False)

        init_db(pipeline_db)
        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)

        conn = duckdb.connect(pipeline_db)
        null_count = conn.execute(
            "SELECT count(*) FROM sales WHERE _ingested_at IS NULL"
        ).fetchone()[0]
        conn.close()
        assert null_count == 0
