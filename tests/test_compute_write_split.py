"""Tests for the compute/write split in reports/runner.py.

CLAUDE.md Test Coverage Gap #8 — _compute_report / _write_report guarantees:

  1. _compute_report returns ReportBundle(status="skipped") when no pending
     records exist — no DB writes occur.
  2. _compute_report returns ReportBundle(status="error") when source table
     is missing — no DB writes occur.
  3. _write_report called with a skipped or error bundle returns immediately
     without touching any DB table.
  4. Parallel run_all_reports produces identical validation_pass and
     validation_block state as sequential for the same input.
  5. _update_report_table is called only on first run; never on subsequent runs.

CLAUDE.md Deferred Work (rule 13):
  - vp validate CLI smoke test: full call chain from Click entry point through
    run_all_reports exits without unexpected exceptions.
"""
import copy
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest

from proto_pipe.checks.registry import CheckRegistry, ReportRegistry
from proto_pipe.io.db import ensure_pipeline_tables
from proto_pipe.io.registry import register_from_config
from proto_pipe.pipelines.watermark import WatermarkStore
from proto_pipe.reports.runner import (
    ReportBundle,
    _compute_report,
    _write_report,
    run_all_reports,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _init_db(pipeline_db: str) -> None:
    """Bootstrap pipeline tables in a fresh DB."""
    conn = duckdb.connect(pipeline_db)
    ensure_pipeline_tables(conn)
    conn.close()


def _seed(pipeline_db: str, table: str, df: pd.DataFrame) -> None:
    conn = duckdb.connect(pipeline_db)
    conn.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM df')
    conn.close()


def _count(pipeline_db: str, table: str) -> int:
    conn = duckdb.connect(pipeline_db)
    n = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    conn.close()
    return n


def _make_report_config(pipeline_db: str, table: str, pk: str) -> dict:
    """Minimal full-path report config (no checks) for compute/write tests."""
    return {
        "name": f"{table}_report",
        "source": {
            "type": "duckdb",
            "path": pipeline_db,
            "table": table,
            "primary_key": pk,
            "timestamp_col": "updated_at",
        },
        "options": {},
        "checks": [],
        "resolved_checks": [],
        "alias_map": [],
    }


def _simple_reports_config(pipeline_db: str) -> dict:
    """Two-report config with a single null check each — no schema check."""
    return {
        "templates": {},
        "reports": [
            {
                "name": "sales_validation",
                "source": {
                    "type": "duckdb",
                    "path": pipeline_db,
                    "table": "sales",
                    "primary_key": "order_id",
                    "timestamp_col": "updated_at",
                },
                "options": {"parallel": False},
                "checks": [{"name": "null_check", "params": {}}],
            },
            {
                "name": "inventory_validation",
                "source": {
                    "type": "duckdb",
                    "path": pipeline_db,
                    "table": "inventory",
                    "primary_key": "sku",
                    "timestamp_col": "updated_at",
                },
                "options": {"parallel": False},
                "checks": [{"name": "null_check", "params": {}}],
            },
        ],
    }


# ---------------------------------------------------------------------------
# Gap 8.1 — _compute_report returns status="skipped" when no pending records
# ---------------------------------------------------------------------------

class TestComputeReportSkipped:
    """CLAUDE.md gap 8: _compute_report returns status='skipped' when no pending records."""

    def test_returns_skipped_after_all_records_validated(
        self, pipeline_db, watermark_store, sales_df
    ):
        """Second compute run on unchanged data must return status='skipped'."""
        _init_db(pipeline_db)
        _seed(pipeline_db, "sales", sales_df)

        config = _make_report_config(pipeline_db, "sales", "order_id")
        cr = CheckRegistry()

        # First run — computes and writes
        bundle1 = _compute_report(config, cr, watermark_store, pipeline_db)
        assert bundle1.status == "computed"

        write_conn = duckdb.connect(pipeline_db)
        _write_report(write_conn, bundle1, watermark_store)
        write_conn.close()

        # Second run — all PKs already in validation_pass with matching hashes
        bundle2 = _compute_report(config, cr, watermark_store, pipeline_db)
        assert bundle2.status == "skipped", (
            "CLAUDE.md gap 8: _compute_report must return status='skipped' "
            "when all records are already in validation_pass"
        )

    def test_skipped_bundle_carries_no_data(
        self, pipeline_db, watermark_store, sales_df
    ):
        """A skipped bundle must not carry pending_pks, flag_records, or pass_entries."""
        _init_db(pipeline_db)
        _seed(pipeline_db, "sales", sales_df)
        config = _make_report_config(pipeline_db, "sales", "order_id")
        cr = CheckRegistry()

        bundle1 = _compute_report(config, cr, watermark_store, pipeline_db)
        write_conn = duckdb.connect(pipeline_db)
        _write_report(write_conn, bundle1, watermark_store)
        write_conn.close()

        bundle2 = _compute_report(config, cr, watermark_store, pipeline_db)
        assert bundle2.status == "skipped"
        assert not bundle2.pending_pks, "Skipped bundle must have no pending_pks"
        assert not bundle2.flag_records, "Skipped bundle must have no flag_records"
        assert not bundle2.pass_entries, "Skipped bundle must have no pass_entries"


# ---------------------------------------------------------------------------
# Gap 8.2 — _compute_report returns status="error" when source table missing
# ---------------------------------------------------------------------------

class TestComputeReportError:
    """CLAUDE.md gap 8: _compute_report returns status='error' when source table missing."""

    def test_returns_error_when_source_table_does_not_exist(
        self, pipeline_db, watermark_store
    ):
        """Missing source table must produce an error bundle — not raise."""
        _init_db(pipeline_db)

        config = _make_report_config(pipeline_db, "nonexistent_table", "id")
        cr = CheckRegistry()

        bundle = _compute_report(config, cr, watermark_store, pipeline_db)

        assert bundle.status == "error", (
            "CLAUDE.md gap 8: _compute_report must return status='error' "
            "when source table is missing, not raise"
        )
        assert bundle.error is not None, "Error bundle must carry a non-None error message"

    def test_error_bundle_carries_no_data(self, pipeline_db, watermark_store):
        """Error bundle must carry no data that could be mistakenly written."""
        _init_db(pipeline_db)
        config = _make_report_config(pipeline_db, "nonexistent_table", "id")
        cr = CheckRegistry()

        bundle = _compute_report(config, cr, watermark_store, pipeline_db)
        assert bundle.status == "error"
        assert bundle.modified_df is None
        assert not bundle.flag_records
        assert not bundle.pass_entries

    def test_no_db_writes_during_compute_phase(
        self, pipeline_db, watermark_store, sales_df
    ):
        """After _compute_report (even with check failures), validation_block is empty.

        The compute phase accumulates FlagRecord objects in memory only.
        Nothing is written to the DB until _write_report is called.
        """
        _init_db(pipeline_db)
        _seed(pipeline_db, "sales", sales_df)
        config = _make_report_config(pipeline_db, "sales", "order_id")
        cr = CheckRegistry()

        _compute_report(config, cr, watermark_store, pipeline_db)

        assert _count(pipeline_db, "validation_block") == 0, (
            "CLAUDE.md gap 8: compute phase must not write to validation_block — "
            "flags are accumulated in ReportBundle.flag_records until _write_report"
        )
        assert _count(pipeline_db, "validation_pass") == 0, (
            "Compute phase must not write to validation_pass"
        )


# ---------------------------------------------------------------------------
# Gap 8.3 — _write_report does not touch DB on skipped or error bundles
# ---------------------------------------------------------------------------

class TestWriteReportGuard:
    """CLAUDE.md gap 8: _write_report must not touch any DB table on skipped/error bundles."""

    def test_skipped_bundle_leaves_validation_block_empty(
        self, pipeline_db, watermark_store
    ):
        """_write_report with status='skipped' must not write to validation_block."""
        _init_db(pipeline_db)

        conn = duckdb.connect(pipeline_db)
        bundle = ReportBundle(report_name="test_report", status="skipped")
        _write_report(conn, bundle, watermark_store)
        conn.close()

        assert _count(pipeline_db, "validation_block") == 0, (
            "CLAUDE.md gap 8: _write_report must not write to validation_block "
            "when bundle.status='skipped'"
        )

    def test_skipped_bundle_leaves_validation_pass_empty(
        self, pipeline_db, watermark_store
    ):
        """_write_report with status='skipped' must not write to validation_pass."""
        _init_db(pipeline_db)

        conn = duckdb.connect(pipeline_db)
        bundle = ReportBundle(report_name="test_report", status="skipped")
        _write_report(conn, bundle, watermark_store)
        conn.close()

        assert _count(pipeline_db, "validation_pass") == 0

    def test_error_bundle_leaves_db_untouched(
        self, pipeline_db, watermark_store
    ):
        """_write_report with status='error' must not modify any pipeline table."""
        _init_db(pipeline_db)

        conn = duckdb.connect(pipeline_db)
        bundle = ReportBundle(
            report_name="test_report",
            status="error",
            error="source table missing",
        )
        result = _write_report(conn, bundle, watermark_store)
        conn.close()

        assert result["status"] == "error"
        assert _count(pipeline_db, "validation_block") == 0
        assert _count(pipeline_db, "validation_pass") == 0

    def test_skipped_bundle_returns_correct_dict(
        self, pipeline_db, watermark_store
    ):
        """_write_report must return the correct status dict for a skipped bundle."""
        _init_db(pipeline_db)
        conn = duckdb.connect(pipeline_db)
        bundle = ReportBundle(report_name="test_report", status="skipped")
        result = _write_report(conn, bundle, watermark_store)
        conn.close()

        assert result["report"] == "test_report"
        assert result["status"] == "skipped"

    def test_error_bundle_returns_correct_dict(
        self, pipeline_db, watermark_store
    ):
        """_write_report must return the correct status dict for an error bundle."""
        _init_db(pipeline_db)
        conn = duckdb.connect(pipeline_db)
        bundle = ReportBundle(
            report_name="test_report",
            status="error",
            error="boom",
        )
        result = _write_report(conn, bundle, watermark_store)
        conn.close()

        assert result["report"] == "test_report"
        assert result["status"] == "error"
        assert result["error"] == "boom"


# ---------------------------------------------------------------------------
# Gap 8.4 — Parallel and sequential produce identical DB state
# ---------------------------------------------------------------------------

class TestParallelVsSequentialDbState:
    """CLAUDE.md gap 8: parallel run_all_reports produces identical validation_pass
    and validation_block state as sequential for the same input.

    Two isolated pipeline DBs are seeded identically. Sequential runs on one;
    parallel on the other. Key columns (excluding timestamps) must be equal.
    """

    def _setup(
        self,
        pipeline_db: str,
        sales_df: pd.DataFrame,
        inventory_df: pd.DataFrame,
        watermark_db: str,
    ) -> tuple:
        _init_db(pipeline_db)
        _seed(pipeline_db, "sales", sales_df)
        _seed(pipeline_db, "inventory", inventory_df)
        cfg = _simple_reports_config(pipeline_db)
        cr, rr = CheckRegistry(), ReportRegistry()
        register_from_config(cfg, cr, rr)
        ws = WatermarkStore(watermark_db)
        return cr, rr, ws

    def test_validation_pass_identical(
        self, tmp_path, sales_df, inventory_df
    ):
        """validation_pass rows must be identical between sequential and parallel runs."""
        seq_db = str(tmp_path / "seq.db")
        par_db = str(tmp_path / "par.db")

        cr1, rr1, ws1 = self._setup(
            seq_db, sales_df, inventory_df, str(tmp_path / "w1.db")
        )
        run_all_reports(rr1, cr1, ws1, parallel_reports=False, pipeline_db=seq_db)

        cr2, rr2, ws2 = self._setup(
            par_db, sales_df, inventory_df, str(tmp_path / "w2.db")
        )
        run_all_reports(rr2, cr2, ws2, parallel_reports=True, pipeline_db=par_db)

        # Compare key columns — exclude validated_at (timestamp differs between runs)
        conn_s = duckdb.connect(seq_db)
        conn_p = duckdb.connect(par_db)
        try:
            seq_pass = conn_s.execute(
                "SELECT pk_value, table_name, report_name, status "
                "FROM validation_pass ORDER BY report_name, pk_value"
            ).df()
            par_pass = conn_p.execute(
                "SELECT pk_value, table_name, report_name, status "
                "FROM validation_pass ORDER BY report_name, pk_value"
            ).df()
        finally:
            conn_s.close()
            conn_p.close()

        pd.testing.assert_frame_equal(
            seq_pass.reset_index(drop=True),
            par_pass.reset_index(drop=True),
            check_like=False,
        )

    def test_validation_block_identical(
        self, tmp_path, sales_df, inventory_df
    ):
        """validation_block rows must be identical between sequential and parallel runs."""
        seq_db = str(tmp_path / "seq.db")
        par_db = str(tmp_path / "par.db")

        cr1, rr1, ws1 = self._setup(
            seq_db, sales_df, inventory_df, str(tmp_path / "w1.db")
        )
        run_all_reports(rr1, cr1, ws1, parallel_reports=False, pipeline_db=seq_db)

        cr2, rr2, ws2 = self._setup(
            par_db, sales_df, inventory_df, str(tmp_path / "w2.db")
        )
        run_all_reports(rr2, cr2, ws2, parallel_reports=True, pipeline_db=par_db)

        # Compare key columns — exclude flagged_at (timestamp differs)
        conn_s = duckdb.connect(seq_db)
        conn_p = duckdb.connect(par_db)
        try:
            seq_block = conn_s.execute(
                "SELECT table_name, report_name, check_name, pk_value, reason "
                "FROM validation_block ORDER BY report_name, pk_value, check_name"
            ).df()
            par_block = conn_p.execute(
                "SELECT table_name, report_name, check_name, pk_value, reason "
                "FROM validation_block ORDER BY report_name, pk_value, check_name"
            ).df()
        finally:
            conn_s.close()
            conn_p.close()

        pd.testing.assert_frame_equal(
            seq_block.reset_index(drop=True),
            par_block.reset_index(drop=True),
            check_like=False,
        )

    def test_report_tables_identical(
        self, tmp_path, sales_df, inventory_df
    ):
        """Report tables must contain identical rows after sequential vs parallel runs."""
        seq_db = str(tmp_path / "seq.db")
        par_db = str(tmp_path / "par.db")

        cr1, rr1, ws1 = self._setup(
            seq_db, sales_df, inventory_df, str(tmp_path / "w1.db")
        )
        run_all_reports(rr1, cr1, ws1, parallel_reports=False, pipeline_db=seq_db)

        cr2, rr2, ws2 = self._setup(
            par_db, sales_df, inventory_df, str(tmp_path / "w2.db")
        )
        run_all_reports(rr2, cr2, ws2, parallel_reports=True, pipeline_db=par_db)

        for table in ("sales_report", "inventory_report"):
            conn_s = duckdb.connect(seq_db)
            conn_p = duckdb.connect(par_db)
            try:
                seq_df = conn_s.execute(
                    f'SELECT * FROM "{table}" ORDER BY 1'
                ).df()
                par_df = conn_p.execute(
                    f'SELECT * FROM "{table}" ORDER BY 1'
                ).df()
                pd.testing.assert_frame_equal(
                    seq_df.reset_index(drop=True),
                    par_df.reset_index(drop=True),
                    check_like=True,
                )
            finally:
                conn_s.close()
                conn_p.close()


# ---------------------------------------------------------------------------
# Gap 8.5 — _update_report_table called only on first run
# ---------------------------------------------------------------------------

class TestUpdateReportTableFirstRunOnly:
    """CLAUDE.md gap 8: _update_report_table must be called on first run and
    never on subsequent runs.

    First run: CREATE TABLE by copying the source table in DuckDB directly,
    then _update_report_table to apply transforms. Subsequent run: DELETE +
    INSERT modified_df — transforms already applied before write, no UPDATE needed.
    """

    def test_update_report_table_called_once_on_first_run(
        self, pipeline_db, watermark_store, sales_df
    ):
        """_update_report_table must be called exactly once when first_run=True."""
        _init_db(pipeline_db)
        conn = duckdb.connect(pipeline_db)
        # Seed source table so _write_report can CREATE TABLE ... AS SELECT * FROM "sales"
        conn.execute("CREATE TABLE sales AS SELECT * FROM sales_df")

        bundle = ReportBundle(
            report_name="test",
            status="computed",
            target_table="sales_report",
            source_table="sales",
            row_count=len(sales_df),
            modified_df=sales_df,
            first_run=True,
            pending_pks=set(sales_df["order_id"].astype(str)),
            pk_col="order_id",
        )

        with patch("proto_pipe.reports.runner._update_report_table") as mock_update:
            _write_report(conn, bundle, watermark_store)
            assert mock_update.call_count == 1, (
                "CLAUDE.md gap 8: _update_report_table must be called exactly "
                "once on first run to apply transform results to the new table"
            )
        conn.close()

    def test_update_report_table_not_called_on_subsequent_run(
        self, pipeline_db, watermark_store, sales_df
    ):
        """_update_report_table must NOT be called when first_run=False."""
        _init_db(pipeline_db)
        conn = duckdb.connect(pipeline_db)
        # Simulate a prior first run — report table already exists
        conn.execute("CREATE TABLE sales_report AS SELECT * FROM sales_df")

        subsequent_row = sales_df.iloc[[0]].copy()
        bundle = ReportBundle(
            report_name="test",
            status="computed",
            target_table="sales_report",
            source_table="sales",
            modified_df=subsequent_row,
            first_run=False,
            pending_pks={str(sales_df.iloc[0]["order_id"])},
            pk_col="order_id",
        )

        with patch("proto_pipe.reports.runner._update_report_table") as mock_update:
            _write_report(conn, bundle, watermark_store)
            assert mock_update.call_count == 0, (
                "CLAUDE.md gap 8: _update_report_table must NOT be called on "
                "subsequent runs — transforms are already applied in modified_df "
                "before the write phase"
            )
        conn.close()

    def test_subsequent_run_data_reflects_modified_df(
        self, pipeline_db, watermark_store, sales_df
    ):
        """After a subsequent run, the report table must contain the data from
        modified_df exactly — confirming transforms applied before write are
        persisted correctly without a separate UPDATE step."""
        _init_db(pipeline_db)
        conn = duckdb.connect(pipeline_db)
        # Seed source table so _write_report can CREATE TABLE ... AS SELECT * FROM "sales"
        conn.execute("CREATE TABLE sales AS SELECT * FROM sales_df")

        # First run — creates the report table from the full source
        first_bundle = ReportBundle(
            report_name="test",
            status="computed",
            target_table="sales_report",
            source_table="sales",
            row_count=len(sales_df),
            modified_df=sales_df,
            first_run=True,
            pending_pks=set(sales_df["order_id"].astype(str)),
            pk_col="order_id",
        )
        _write_report(conn, first_bundle, watermark_store)

        # Subsequent run — ORD-001 with a modified price (simulates transform result)
        changed = sales_df[sales_df["order_id"] == "ORD-001"].copy()
        changed.loc[changed.index[0], "price"] = 999.99

        sub_bundle = ReportBundle(
            report_name="test",
            status="computed",
            target_table="sales_report",
            source_table="sales",
            modified_df=changed,
            first_run=False,
            pending_pks={"ORD-001"},
            pk_col="order_id",
        )
        _write_report(conn, sub_bundle, watermark_store)

        updated_price = conn.execute(
            "SELECT price FROM sales_report WHERE order_id = 'ORD-001'"
        ).fetchone()[0]
        conn.close()

        assert updated_price == 999.99, (
            "Report table must reflect modified_df values after subsequent run "
            "— confirming DELETE+INSERT of post-transform data is sufficient"
        )


# ---------------------------------------------------------------------------
# Rule 13 — vp validate CLI smoke test
# ---------------------------------------------------------------------------

class TestVpValidateSmoke:
    """CLAUDE.md Deferred Work (rule 13): vp validate CLI smoke test.

    Verifies the full call chain from the Click entry point:
      vp validate → load_config → register_from_config →
      run_all_reports (_compute_report + _write_report) →
      write_pipeline_events → validation_block summary

    Only load_custom_checks is mocked (no user module available in test env).
    All other code — Click wiring, registries, DuckDB, watermark — runs for real.
    """

    def test_exits_cleanly_with_empty_reports_config(self, tmp_path):
        """vp validate with no configured reports must exit with code 0.

        Exercises: Click entry → load config → register_from_config (no-op) →
        run_all_reports (empty layers) → write_pipeline_events → summary query.
        """
        from click.testing import CliRunner
        from proto_pipe.cli.commands.validation import validate

        pipeline_db = str(tmp_path / "pipeline.db")
        watermark_db = str(tmp_path / "watermarks.db")
        reports_cfg = tmp_path / "reports_config.yaml"
        reports_cfg.write_text("templates: {}\nreports: []\n")

        runner = CliRunner()
        with patch("proto_pipe.checks.helpers.load_custom_checks"):
            result = runner.invoke(validate, [
                "--pipeline-db", pipeline_db,
                "--watermark-db", watermark_db,
                "--reports-config", str(reports_cfg),
            ])

        assert result.exit_code == 0, (
            f"vp validate with empty config must exit 0.\n"
            f"Output:\n{result.output}"
        )

    def test_exits_cleanly_with_unknown_table_filter(self, tmp_path):
        """vp validate --table <no match> must exit 0 with a warning, not crash."""
        from click.testing import CliRunner
        from proto_pipe.cli.commands.validation import validate

        pipeline_db = str(tmp_path / "pipeline.db")
        watermark_db = str(tmp_path / "watermarks.db")
        reports_cfg = tmp_path / "reports_config.yaml"
        reports_cfg.write_text("templates: {}\nreports: []\n")

        runner = CliRunner()
        with patch("proto_pipe.checks.helpers.load_custom_checks"):
            result = runner.invoke(validate, [
                "--pipeline-db", pipeline_db,
                "--watermark-db", watermark_db,
                "--reports-config", str(reports_cfg),
                "--table", "nonexistent_table",
            ])

        assert result.exit_code == 0, (
            f"vp validate --table <nomatch> must exit 0.\n"
            f"Output:\n{result.output}"
        )

    def test_full_chain_with_real_data_exits_cleanly(self, tmp_path, sales_df):
        """Full call chain with one real report and real source data must exit 0.

        This is the primary rule 13 integration test. The entire chain from
        Click entry through DuckDB writes must execute without raising.
        """
        from click.testing import CliRunner
        from proto_pipe.cli.commands.validation import validate

        pipeline_db = str(tmp_path / "pipeline.db")
        watermark_db = str(tmp_path / "watermarks.db")
        reports_cfg = tmp_path / "reports_config.yaml"

        _init_db(pipeline_db)
        _seed(pipeline_db, "sales", sales_df)

        reports_cfg.write_text(
            "templates: {}\n"
            "reports:\n"
            "  - name: sales_validation\n"
            "    source:\n"
            f"      type: duckdb\n"
            f"      path: {pipeline_db}\n"
            f"      table: sales\n"
            f"      primary_key: order_id\n"
            f"      timestamp_col: updated_at\n"
            f"    options:\n"
            f"      parallel: false\n"
            f"    checks: []\n"
        )

        runner = CliRunner()
        with patch("proto_pipe.checks.helpers.load_custom_checks"):
            result = runner.invoke(validate, [
                "--pipeline-db", pipeline_db,
                "--watermark-db", watermark_db,
                "--reports-config", str(reports_cfg),
            ])

        assert result.exit_code == 0, (
            f"vp validate with real data must exit 0.\n"
            f"Output:\n{result.output}"
        )
        assert "sales_validation" in result.output, (
            "Report name must appear in vp validate output"
        )
