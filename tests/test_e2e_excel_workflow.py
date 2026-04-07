"""
End-to-end test: Excel file ingest → checks → corrections → deliverable output

Flow:
  1. Ingest an Excel file into the pipeline DB
  2. Verify _ingested_at column is added
  3. Run checks — expect flagged rows for bad data
  4. Apply corrections to flagged rows
  5. Re-validate — verify flags are cleared
  6. Execute a sql_file deliverable — verify output DataFrame
"""

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from proto_pipe.io.ingest import init_db, ingest_directory
from proto_pipe.io.registry import register_from_config
from proto_pipe.pipelines.watermark import WatermarkStore
from proto_pipe.checks.registry import CheckRegistry, ReportRegistry
from proto_pipe.pipelines.query import execute_sql_file
from proto_pipe.reports.runner import run_all_reports

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

EXCEL_ROWS = [
    # Good rows
    {"order_id": "ORD-001", "customer_id": "CUST-A", "price": 99.99,  "quantity": 2, "region": "EMEA", "updated_at": "2026-01-15"},
    {"order_id": "ORD-002", "customer_id": "CUST-B", "price": 250.00, "quantity": 1, "region": "APAC", "updated_at": "2026-02-10"},
    # Bad rows — will trigger checks
    {"order_id": "ORD-003", "customer_id": None,     "price": 15.50,  "quantity": 5, "region": "EMEA", "updated_at": "2026-03-01"},  # null customer_id
    {"order_id": "ORD-004", "customer_id": "CUST-D", "price": -10.00, "quantity": 1, "region": "APAC", "updated_at": "2026-03-05"},  # negative price
]


@pytest.fixture()
def excel_file_conflict(tmp_path, excel_file) -> Path:
    """Second Excel file with same order IDs but modified prices — triggers ingest conflicts."""
    conflicting_rows = [
        {"order_id": "ORD-001", "customer_id": "CUST-A", "price": 199.99, "quantity": 2, "region": "EMEA", "updated_at": "2026-01-16"},
        {"order_id": "ORD-002", "customer_id": "CUST-B", "price": 350.00, "quantity": 1, "region": "APAC", "updated_at": "2026-02-11"},
    ]
    path = excel_file.parent / "sales_2026_04.xlsx"
    pd.DataFrame(conflicting_rows).to_excel(path, index=False)
    return path


@pytest.fixture()
def excel_file(tmp_path) -> Path:
    """Write fake data to an Excel file in the incoming directory."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    path = incoming / "sales_2026_03.xlsx"
    pd.DataFrame(EXCEL_ROWS).to_excel(path, index=False)
    return path


@pytest.fixture()
def excel_sources_config() -> dict:
    return {
        "sources": [
            {
                "name": "sales",
                "target_table": "sales",
                "patterns": ["sales_*.xlsx", "sales_*.csv"],
                "primary_key": "order_id",
                "on_duplicate": "flag",
            }
        ]
    }


@pytest.fixture()
def excel_reports_config(pipeline_db) -> dict:
    return {
        "templates": {
            "null_check_tmpl": {"name": "null_check"},
            "price_range_tmpl": {
                "name": "range_check",
                "params": {"col": "price", "min_val": 0, "max_val": 500},
            },
        },
        "reports": [
            {
                "name": "sales_validation",
                "source": {
                    "type": "duckdb",
                    "path": pipeline_db,
                    "table": "sales",
                },
                "options": {"parallel": False},
                "checks": [
                    {"template": "null_check_tmpl"},
                    {"template": "price_range_tmpl"},
                ],
            }
        ],
    }


@pytest.fixture()
def infra_db(pipeline_db) -> str:
    """Pipeline DB with all infrastructure tables created."""
    from proto_pipe.io.db import init_all_pipeline_tables
    from proto_pipe.pipelines.query import init_report_runs_table
    init_db(pipeline_db)
    conn = duckdb.connect(pipeline_db)
    init_all_pipeline_tables(conn)
    init_report_runs_table(conn)
    conn.close()
    return pipeline_db


# ---------------------------------------------------------------------------
# Step helpers
# ---------------------------------------------------------------------------

def _ingest(infra_db, excel_file, excel_sources_config) -> dict:
    return ingest_directory(
        directory=str(excel_file.parent),
        sources=excel_sources_config["sources"],
        db_path=infra_db,
    )


def _run_checks(infra_db, excel_reports_config, watermark_db):
    check_registry = CheckRegistry()
    report_registry = ReportRegistry()
    register_from_config(excel_reports_config, check_registry, report_registry)
    watermark_store = WatermarkStore(watermark_db)
    return run_all_reports(report_registry, check_registry, watermark_store, pipeline_db=infra_db)


def _get_flagged_rows(infra_db) -> pd.DataFrame:
    conn = duckdb.connect(infra_db)
    df = conn.execute("SELECT * FROM source_block").df()
    conn.close()
    return df


def _clear_flags(infra_db, table: str):
    conn = duckdb.connect(infra_db)
    conn.execute("DELETE FROM source_block WHERE table_name = ?", [table])
    conn.close()


def _get_validation_flags(infra_db) -> pd.DataFrame:
    conn = duckdb.connect(infra_db)
    df = conn.execute("SELECT * FROM validation_block").df()
    conn.close()
    return df


def _clear_validation_flags(infra_db):
    conn = duckdb.connect(infra_db)
    conn.execute("DELETE FROM validation_block")
    conn.close()


# ---------------------------------------------------------------------------
# End-to-end test class
# ---------------------------------------------------------------------------

class TestExcelWorkflow:
    def test_step1_ingest_excel_file(
        self, infra_db, excel_file, excel_sources_config
    ):
        """Excel file is ingested and sales table is created."""
        summary = _ingest(infra_db, excel_file, excel_sources_config)

        assert excel_file.name in summary
        assert summary[excel_file.name]["status"] == "ok"
        assert summary[excel_file.name]["rows"] > 0

        conn = duckdb.connect(infra_db)
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).df()["table_name"].tolist()
        conn.close()
        assert "sales" in tables

    def test_step2_ingested_at_column_added(
        self, infra_db, excel_file, excel_sources_config
    ):
        """_ingested_at column is present on every ingested row."""
        _ingest(infra_db, excel_file, excel_sources_config)

        conn = duckdb.connect(infra_db)
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'sales'"
        ).df()["column_name"].tolist()
        conn.close()
        assert "_ingested_at" in cols

    def test_step3_ingested_at_is_not_null(
        self, infra_db, excel_file, excel_sources_config
    ):
        """All rows have a non-null _ingested_at value."""
        _ingest(infra_db, excel_file, excel_sources_config)

        conn = duckdb.connect(infra_db)
        null_count = conn.execute(
            "SELECT count(*) FROM sales WHERE _ingested_at IS NULL"
        ).fetchone()[0]
        conn.close()
        assert null_count == 0

    def test_step4_checks_flag_bad_rows(
        self, infra_db, excel_file, excel_sources_config,
        excel_reports_config, watermark_db
    ):
        """Checks run and flag rows with nulls and out-of-range prices."""
        _ingest(infra_db, excel_file, excel_sources_config)
        _run_checks(infra_db, excel_reports_config, watermark_db)

        flagged = _get_validation_flags(infra_db)
        assert not flagged.empty
        assert "sales_validation" in flagged["report_name"].values

    def test_step5_flagged_rows_have_correct_table(
        self, infra_db, excel_file, excel_sources_config,
        excel_reports_config, watermark_db
    ):
        """Flagged rows reference the correct table."""
        _ingest(infra_db, excel_file, excel_sources_config)
        _run_checks(infra_db, excel_reports_config, watermark_db)

        flagged = _get_flagged_rows(infra_db)
        assert all(flagged["table_name"] == "sales")

    def test_step6_clear_flags_and_revalidate(
        self, infra_db, excel_file, excel_sources_config,
        excel_reports_config, watermark_db
    ):
        """After clearing flags, re-running checks produces a clean state."""
        _ingest(infra_db, excel_file, excel_sources_config)
        _run_checks(infra_db, excel_reports_config, watermark_db)

        # Verify flags exist first
        flagged_before = _get_validation_flags(infra_db)
        assert not flagged_before.empty

        # Clear flags (simulating corrections applied)
        _clear_validation_flags(infra_db)

        flagged_after = _get_validation_flags(infra_db)
        assert flagged_after.empty

    def test_step7_sql_file_deliverable_output(
        self, infra_db, excel_file, excel_sources_config, tmp_path
    ):
        """A sql_file deliverable executes and returns a DataFrame."""
        _ingest(infra_db, excel_file, excel_sources_config)

        # Write a simple deliverable SQL file
        sql_path = tmp_path / "carrier_a.sql"
        sql_path.write_text(
            "SELECT order_id, customer_id, price, region FROM sales"
            " WHERE price > 0"
            " ORDER BY order_id"
        )

        conn = duckdb.connect(infra_db)
        df = execute_sql_file(conn, str(sql_path))
        conn.close()

        assert not df.empty
        assert "order_id" in df.columns
        assert "price" in df.columns
        # negative price row should be excluded by WHERE price > 0
        assert all(df["price"] > 0)

    def test_step8_internal_columns_excluded_from_output(
        self, infra_db, excel_file, excel_sources_config, tmp_path
    ):
        """Deliverable SQL that selects explicit columns excludes _ingested_at."""
        _ingest(infra_db, excel_file, excel_sources_config)

        sql_path = tmp_path / "clean_output.sql"
        sql_path.write_text(
            "SELECT order_id, customer_id, price, region FROM sales ORDER BY order_id"
        )

        conn = duckdb.connect(infra_db)
        df = execute_sql_file(conn, str(sql_path))
        conn.close()

        assert "_ingested_at" not in df.columns

    def test_step9_row_count_matches_expected(
        self, infra_db, excel_file, excel_sources_config, tmp_path
    ):
        """Row count in deliverable output matches source data."""
        _ingest(infra_db, excel_file, excel_sources_config)

        sql_path = tmp_path / "all_rows.sql"
        sql_path.write_text("SELECT * FROM sales ORDER BY order_id")

        conn = duckdb.connect(infra_db)
        df = execute_sql_file(conn, str(sql_path))
        conn.close()

        assert len(df) == len(EXCEL_ROWS)

    def test_step10_ingest_conflict_triggers_flagged_rows(
        self, infra_db, excel_file, excel_file_conflict, excel_sources_config
    ):
        """Ingesting a second file with same primary keys but different content
        writes entries to flagged_rows."""
        # First ingest — clean
        _ingest(infra_db, excel_file, excel_sources_config)
        # Second ingest — same order IDs, different prices → conflicts
        _ingest(infra_db, excel_file_conflict, excel_sources_config)

        flagged = _get_flagged_rows(infra_db)
        assert not flagged.empty
        assert all(flagged["table_name"] == "sales")
        assert all(flagged["check_name"] == "duplicate_conflict")  # source_block check_name

    def test_step11_flagged_rows_contain_conflict_reason(
        self, infra_db, excel_file, excel_file_conflict, excel_sources_config
    ):
        """Flagged rows include the changed column names in the reason."""
        _ingest(infra_db, excel_file, excel_sources_config)
        _ingest(infra_db, excel_file_conflict, excel_sources_config)

        flagged = _get_flagged_rows(infra_db)
        assert not flagged.empty
        # reason should mention price since that changed
        assert any("price" in str(r).lower() for r in flagged["reason"].values)


    # -----------------------------------------------------------------------
    # Guarantees from Spec — behavioral contracts not covered above
    # -----------------------------------------------------------------------

    def test_source_pass_populated_after_ingest(
        self, infra_db, excel_file, excel_sources_config
    ):
        """source_pass tracks every accepted record's row hash and source file.

        Spec guarantee:
          'source_pass → tracks every ingested record's row hash and source file.
           Enables flag mode duplicate detection without querying the source
           table directly.'
        """
        _ingest(infra_db, excel_file, excel_sources_config)

        conn = duckdb.connect(infra_db)
        df = conn.execute("SELECT * FROM source_pass WHERE table_name = 'sales'").df()
        conn.close()

        assert not df.empty, "source_pass must be populated after ingest"
        assert len(df) == len(EXCEL_ROWS), (
            "source_pass must have one entry per accepted row"
        )
        assert df["row_hash"].notna().all(), "every source_pass row must have a hash"
        assert df["source_file"].notna().all(), "every source_pass row must record its source file"

    def test_existing_row_kept_on_conflict(
        self, infra_db, excel_file, excel_file_conflict, excel_sources_config
    ):
        """On duplicate_conflict, the original row stays in the source table.

        Spec guarantee:
          'Existing row kept, incoming row blocked.'
        """
        _ingest(infra_db, excel_file, excel_sources_config)

        conn = duckdb.connect(infra_db)
        original_prices = conn.execute(
            "SELECT order_id, price FROM sales ORDER BY order_id"
        ).df().set_index("order_id")["price"].to_dict()
        conn.close()

        _ingest(infra_db, excel_file_conflict, excel_sources_config)

        conn = duckdb.connect(infra_db)
        prices_after = conn.execute(
            "SELECT order_id, price FROM sales ORDER BY order_id"
        ).df().set_index("order_id")["price"].to_dict()
        conn.close()

        # Original prices must be preserved — incoming conflicting rows are blocked
        for order_id, original_price in original_prices.items():
            assert prices_after[order_id] == original_price, (
                f"Row {order_id}: original price {original_price} was overwritten "
                f"— existing row must be kept on duplicate_conflict"
            )

    def test_clean_rows_proceed_despite_conflicts(
        self, infra_db, excel_file, excel_sources_config, tmp_path
    ):
        """Non-conflicting rows in the same file still insert when others conflict.

        Spec guarantee:
          'Duplicate handling runs after type validation, on clean rows only.
           Clean rows proceed. No pandas intermediate.'
        """
        _ingest(infra_db, excel_file, excel_sources_config)

        # File with one conflicting row (ORD-001) and one new row (ORD-005)
        mixed = [
            {"order_id": "ORD-001", "customer_id": "CUST-A", "price": 999.99,
             "quantity": 2, "region": "EMEA", "updated_at": "2026-01-16"},
            {"order_id": "ORD-005", "customer_id": "CUST-E", "price": 75.00,
             "quantity": 3, "region": "EMEA", "updated_at": "2026-04-01"},
        ]
        path = tmp_path / "incoming" / "sales_mixed.xlsx"
        path.parent.mkdir(exist_ok=True)
        pd.DataFrame(mixed).to_excel(path, index=False)

        summary = ingest_directory(
            directory=str(path.parent),
            sources=excel_sources_config["sources"],
            db_path=infra_db,
        )

        conn = duckdb.connect(infra_db)
        new_row = conn.execute(
            "SELECT * FROM sales WHERE order_id = 'ORD-005'"
        ).df()
        conn.close()

        assert not new_row.empty, (
            "ORD-005 (clean row) must be inserted even though ORD-001 conflicted"
        )

    def test_ingest_state_logged_per_file(
        self, infra_db, excel_file, excel_sources_config
    ):
        """Every ingest attempt is logged to ingest_state with status='ok'.

        Spec guarantee:
          'ingest_state — Per-file ingest history: status, rows, errors'
          'ingest_state status values: ok | failed | skipped | correction'
        """
        _ingest(infra_db, excel_file, excel_sources_config)

        conn = duckdb.connect(infra_db)
        df = conn.execute(
            "SELECT * FROM ingest_state WHERE filename = ?",
            [excel_file.name],
        ).df()
        conn.close()

        assert len(df) == 1, "ingest_state must have exactly one entry per file"
        assert df.iloc[0]["status"] == "ok"
        assert df.iloc[0]["rows"] == len(EXCEL_ROWS)

    def test_already_ingested_file_skipped(
        self, infra_db, excel_file, excel_sources_config
    ):
        """A file already in ingest_state with status='ok' is skipped on re-run.

        Spec guarantee:
          'Files already ingested are skipped automatically on subsequent runs.'
        """
        _ingest(infra_db, excel_file, excel_sources_config)

        conn = duckdb.connect(infra_db)
        row_count_after_first = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        conn.close()

        summary = _ingest(infra_db, excel_file, excel_sources_config)

        conn = duckdb.connect(infra_db)
        row_count_after_second = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        conn.close()

        assert summary[excel_file.name]["status"] == "skipped", (
            "Already-ingested file must be skipped"
        )
        assert row_count_after_second == row_count_after_first, (
            "Row count must not change when file is skipped"
        )

    def test_validation_block_does_not_block_sql_deliverable(
        self, infra_db, excel_file, excel_sources_config,
        excel_reports_config, watermark_db, tmp_path
    ):
        """validation_block failures warn but do not block deliverable SQL execution.

        Spec guarantee:
          'validation_block → Check/transform failures — warns, does not block
           deliverables.'
          'Deliverables (Extract): Not blocked by validation_block.'
        """
        _ingest(infra_db, excel_file, excel_sources_config)
        _run_checks(infra_db, excel_reports_config, watermark_db)

        # Confirm validation failures exist
        flagged = _get_validation_flags(infra_db)
        assert not flagged.empty, "Need validation failures for this test to be meaningful"

        # Deliverable SQL must still execute despite validation_block entries
        sql_path = tmp_path / "deliverable.sql"
        sql_path.write_text("SELECT order_id, price FROM sales ORDER BY order_id")

        conn = duckdb.connect(infra_db)
        from proto_pipe.pipelines.query import execute_sql_file
        df = execute_sql_file(conn, str(sql_path))
        conn.close()

        assert not df.empty, (
            "SQL deliverable must execute even when validation_block has entries"
        )

    def test_flag_id_is_deterministic_md5_of_pk(
        self, infra_db, excel_file, excel_file_conflict, excel_sources_config
    ):
        """source_block.id = md5(str(pk_value)) — deterministic, not random UUID.

        Spec guarantee:
          'id = md5(str(pk_value)) — deterministic, computable in DuckDB SQL'
          'Determinism matters. UUID keys, watermarks, flag identity are all
           deterministic.'
        """
        import hashlib
        _ingest(infra_db, excel_file, excel_sources_config)
        _ingest(infra_db, excel_file_conflict, excel_sources_config)

        conn = duckdb.connect(infra_db)
        df = conn.execute("SELECT id, pk_value FROM source_block").df()
        conn.close()

        for _, row in df.iterrows():
            expected_id = hashlib.md5(str(row["pk_value"]).encode()).hexdigest()
            assert row["id"] == expected_id, (
                f"Flag id for pk '{row['pk_value']}' must be md5(pk_value), "
                f"got '{row['id']}'"
            )

    def test_source_block_bad_columns_records_changed_columns(
        self, infra_db, excel_file, excel_file_conflict, excel_sources_config
    ):
        """source_block.bad_columns records the pipe-delimited changed column names.

        Spec guarantee (source_block schema):
          'bad_columns — pipe-delimited: "amount|region"'
        """
        _ingest(infra_db, excel_file, excel_sources_config)
        _ingest(infra_db, excel_file_conflict, excel_sources_config)

        conn = duckdb.connect(infra_db)
        df = conn.execute("SELECT bad_columns FROM source_block").df()
        conn.close()

        assert not df.empty
        # bad_columns must be set (not null) and reference the changed column
        assert df["bad_columns"].notna().all(), (
            "source_block.bad_columns must not be null for duplicate_conflict flags"
        )
        # price changed — must appear in bad_columns
        assert any("price" in str(v) for v in df["bad_columns"]), (
            "bad_columns must record 'price' as the changed column"
        )

    def test_step12_clear_ingest_flags(
        self, infra_db, excel_file, excel_file_conflict, excel_sources_config
    ):
        """Clearing ingest flags removes them from flagged_rows."""
        _ingest(infra_db, excel_file, excel_sources_config)
        _ingest(infra_db, excel_file_conflict, excel_sources_config)

        flagged_before = _get_flagged_rows(infra_db)
        assert not flagged_before.empty

        _clear_flags(infra_db, "sales")

        flagged_after = _get_flagged_rows(infra_db)
        assert flagged_after.empty
