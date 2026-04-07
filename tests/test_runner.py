"""
Tests for validation_pipeline.runner

Covers:
- run_report: skips when no new data, completes when data present
- run_report: watermark advanced after successful run
- run_report: watermark NOT advanced when checks fail hard
- run_all_reports: returns results for all registered reports
- Parallel and sequential execution both produce correct results
"""

from functools import partial

import duckdb

from proto_pipe.checks.built_in import check_nulls, check_range
from proto_pipe.io.registry import register_from_config
from proto_pipe.pipelines.watermark import WatermarkStore
from proto_pipe.checks.registry import CheckRegistry, ReportRegistry
from proto_pipe.reports.runner import run_report, run_all_reports


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_report_config(pipeline_db: str, table: str, check_names: list[str]) -> dict:
    return {
        "name": f"{table}_validation",
        "source": {
            "type": "duckdb",
            "path": pipeline_db,
            "table": table,
            "timestamp_col": "updated_at",
        },
        "options": {"parallel": False},
        "resolved_checks": check_names,
    }


def _seed_table(pipeline_db: str, table: str, df) -> None:
    conn = duckdb.connect(pipeline_db)
    conn.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM df')
    conn.close()


# ---------------------------------------------------------------------------
# run_report — basic behaviour
# ---------------------------------------------------------------------------

class TestRunReport:
    def test_completes_with_clean_data(
        self, pipeline_db, watermark_db, sales_df, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        check_registry.register("null_check", check_nulls)
        config = _make_report_config(pipeline_db, "sales", ["null_check"])

        result = run_report(config, check_registry, watermark_store)

        assert result["status"] == "completed"
        assert result["results"]["null_check"].status == "passed"

    def test_skips_when_no_new_data(
        self, pipeline_db, watermark_db, sales_df, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        check_registry.register("null_check", check_nulls)
        config = _make_report_config(pipeline_db, "sales", ["null_check"])

        # First run advances the watermark past all rows
        run_report(config, check_registry, watermark_store)
        # Second run — no new rows
        result = run_report(config, check_registry, watermark_store)

        assert result["status"] == "skipped"

    def test_check_failure_is_captured_not_raised(
        self, pipeline_db, sales_df_out_of_range, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df_out_of_range)

        # range_check with params baked in via partial
        fn = partial(check_range, col="price", min_val=0, max_val=500)
        check_registry.register("price_range", fn)
        config = _make_report_config(pipeline_db, "sales", ["price_range"])

        result = run_report(config, check_registry, watermark_store)

        assert result["status"] == "completed"
        # The check ran and captured the failure — status must be "failed", not raised
        assert result["results"]["price_range"].status == "failed"

    def test_missing_check_name_marks_report_failed(
        self, pipeline_db, sales_df, check_registry, watermark_store
    ):
        """An unregistered check name must be caught — report completes with check errored.

        CLAUDE.md guarantee: run_check_safe catches exceptions so one failure does not
        halt the report. Unregistered names reach run_check_safe via a guard on get_kind
        in reports/runner.py and return CheckOutcome(status="error").
        """
        _seed_table(pipeline_db, "sales", sales_df)
        # "nonexistent_check" is never registered
        config = _make_report_config(pipeline_db, "sales", ["nonexistent_check"])

        result = run_report(config, check_registry, watermark_store)

        # run_check_safe catches the ValueError and returns status="error" — report
        # completes without raising. The check outcome is "error", not "failed",
        # because the check never ran (it wasn't registered).
        assert result["status"] == "completed"
        assert result["results"]["nonexistent_check"].status == "error"


# ---------------------------------------------------------------------------
# Watermark advancement
# ---------------------------------------------------------------------------

class TestWatermarkAdvancement:
    def test_watermark_advances_after_successful_run(
        self, pipeline_db, sales_df, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        check_registry.register("null_check", check_nulls)
        config = _make_report_config(pipeline_db, "sales", ["null_check"])

        assert watermark_store.get("sales_validation") is None
        run_report(config, check_registry, watermark_store)
        assert watermark_store.get("sales_validation") is not None

    def test_watermark_reflects_max_timestamp_in_data(
        self, pipeline_db, sales_df, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        check_registry.register("null_check", check_nulls)
        config = _make_report_config(pipeline_db, "sales", ["null_check"])

        run_report(config, check_registry, watermark_store)
        mark = watermark_store.get("sales_validation")

        # Max updated_at in sales_df is 2026-03-01
        assert mark.year == 2026
        assert mark.month == 3
        assert mark.day == 1


# ---------------------------------------------------------------------------
# run_all_reports
# ---------------------------------------------------------------------------

class TestRunAllReports:
    def test_all_reports_run(
        self, pipeline_db, sales_df, inventory_df,
        check_registry, report_registry, watermark_store, reports_config
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        _seed_table(pipeline_db, "inventory", inventory_df)

        # Patch source paths to point at tmp pipeline_db
        for r in reports_config["reports"]:
            r["source"]["path"] = pipeline_db

        register_from_config(reports_config, check_registry, report_registry)
        results = run_all_reports(
            report_registry, check_registry, watermark_store, parallel_reports=False
        )

        report_names = {r["report"] for r in results}
        assert "daily_sales_validation" in report_names
        assert "inventory_validation" in report_names

    def test_parallel_and_sequential_same_outcome(
        self, pipeline_db, sales_df, inventory_df,
        reports_config, watermark_db
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        _seed_table(pipeline_db, "inventory", inventory_df)

        for r in reports_config["reports"]:
            r["source"]["path"] = pipeline_db

        # Sequential
        cr1 = CheckRegistry()
        rr1 = ReportRegistry()
        ws1 = WatermarkStore(watermark_db)
        register_from_config(reports_config, cr1, rr1)
        seq_results = run_all_reports(rr1, cr1, ws1, parallel_reports=False)

        # Parallel — needs fresh watermark db so it doesn't skip
        from pathlib import Path
        wdb2 = str(Path(watermark_db).parent / "watermarks2.db")
        cr2 = CheckRegistry()
        rr2 = ReportRegistry()
        ws2 = WatermarkStore(wdb2)
        register_from_config(reports_config, cr2, rr2)
        par_results = run_all_reports(rr2, cr2, ws2, parallel_reports=True)

        seq_statuses = {r["report"]: r["status"] for r in seq_results}
        par_statuses = {r["report"]: r["status"] for r in par_results}
        assert seq_statuses == par_statuses


# ---------------------------------------------------------------------------
# CLAUDE.md behavioral guarantee tests — _build_execution_layers
# ---------------------------------------------------------------------------

import pytest

from proto_pipe.reports.runner import _build_execution_layers, _get_target_table


def _report(name: str, source_table: str, target_table: str | None = None) -> dict:
    """Minimal report config for execution layer tests."""
    r = {"name": name, "source": {"table": source_table}, "checks": [], "resolved_checks": []}
    if target_table:
        r["target_table"] = target_table
    return r


class TestBuildExecutionLayersTopologicalSort:
    """_build_execution_layers sorts reports into dependency-ordered layers.

    CLAUDE.md guarantee:
      'Reports in the same layer have no dependencies on each other and may
       run concurrently. Every layer must complete before the next layer begins.
       Reports with no dependencies are always in layer 0.'
    """

    def test_independent_reports_in_single_layer(self):
        reports = [
            _report("sales_validation", "sales"),
            _report("inventory_validation", "inventory"),
        ]
        layers = _build_execution_layers(reports)
        assert len(layers) == 1, "Independent reports must be in a single layer"
        names = {r["name"] for r in layers[0]}
        assert names == {"sales_validation", "inventory_validation"}

    def test_dependent_report_in_later_layer(self):
        """B reads from A's output table → B must be in a later layer than A."""
        reports = [
            _report("a", "raw_sales", target_table="sales_report"),
            _report("b", "sales_report"),
        ]
        layers = _build_execution_layers(reports)
        assert len(layers) == 2, "Dependent report must be in a later layer"
        assert layers[0][0]["name"] == "a"
        assert layers[1][0]["name"] == "b"

    def test_target_table_falls_back_to_source_report(self):
        """Without explicit target_table, falls back to <source_table>_report."""
        reports = [
            _report("a", "raw_sales"),        # target_table → raw_sales_report
            _report("b", "raw_sales_report"),
        ]
        layers = _build_execution_layers(reports)
        assert len(layers) == 2
        assert layers[0][0]["name"] == "a"
        assert layers[1][0]["name"] == "b"

    def test_chain_of_three_in_three_layers(self):
        """A → B → C must produce three sequential layers."""
        reports = [
            _report("a", "raw",   target_table="a_out"),
            _report("b", "a_out", target_table="b_out"),
            _report("c", "b_out"),
        ]
        layers = _build_execution_layers(reports)
        assert len(layers) == 3
        assert layers[0][0]["name"] == "a"
        assert layers[1][0]["name"] == "b"
        assert layers[2][0]["name"] == "c"

    def test_independent_reports_all_in_layer_zero(self):
        """All reports with no upstream dependencies must be in layer 0."""
        reports = [
            _report("x", "table_x"),
            _report("y", "table_y"),
            _report("z", "table_z"),
        ]
        layers = _build_execution_layers(reports)
        assert len(layers) == 1
        assert len(layers[0]) == 3

    def test_diamond_dependency_ordering(self):
        """A → B, A → C, B → D: A must precede B and C; B must precede D."""
        reports = [
            _report("a", "raw",   target_table="a_out"),
            _report("b", "a_out", target_table="b_out"),
            _report("c", "a_out", target_table="c_out"),
            _report("d", "b_out"),
        ]
        layers = _build_execution_layers(reports)
        all_names = [r["name"] for layer in layers for r in layer]
        assert all_names.index("a") < all_names.index("b")
        assert all_names.index("a") < all_names.index("c")
        assert all_names.index("b") < all_names.index("d")

    def test_empty_reports_returns_empty(self):
        assert _build_execution_layers([]) == []

    def test_single_report_returns_one_layer(self):
        layers = _build_execution_layers([_report("solo", "raw_table")])
        assert len(layers) == 1
        assert len(layers[0]) == 1


class TestBuildExecutionLayersCycleDetection:
    """_build_execution_layers raises ValueError before any execution on cycles.

    CLAUDE.md guarantee:
      'If a circular dependency is detected, _build_execution_layers raises
       ValueError naming the reports involved before any execution begins.'
    """

    def test_direct_cycle_raises(self):
        reports = [
            _report("a", "b_out", target_table="a_out"),
            _report("b", "a_out", target_table="b_out"),
        ]
        with pytest.raises(ValueError, match="cycle"):
            _build_execution_layers(reports)

    def test_three_report_cycle_raises(self):
        reports = [
            _report("a", "c_out", target_table="a_out"),
            _report("b", "a_out", target_table="b_out"),
            _report("c", "b_out", target_table="c_out"),
        ]
        with pytest.raises(ValueError, match="cycle"):
            _build_execution_layers(reports)

    def test_cycle_error_names_the_reports(self):
        """Error message must name the reports involved."""
        reports = [
            _report("alpha", "beta_out", target_table="alpha_out"),
            _report("beta",  "alpha_out", target_table="beta_out"),
        ]
        with pytest.raises(ValueError) as exc_info:
            _build_execution_layers(reports)
        msg = str(exc_info.value)
        assert "alpha" in msg and "beta" in msg, (
            "Cycle error must name the reports involved"
        )

    def test_self_reference_excluded_not_a_cycle(self):
        """A report whose source table matches its own output is excluded by design."""
        reports = [_report("a", "a_out", target_table="a_out")]
        # Guard in code: upstream_name != r["name"] → self-loops excluded
        layers = _build_execution_layers(reports)
        assert len(layers) == 1


class TestRunAllReportsFailurePropagation:
    """Dependents are skipped when upstream report errors.

    CLAUDE.md guarantee:
      'If a report finishes with status=error, its direct dependents in later
       layers are skipped (not errored) and logged with a message naming the
       upstream failure.'
    """

    def test_dependent_skipped_when_upstream_errors(
        self, pipeline_db, watermark_store
    ):
        from proto_pipe.checks.registry import CheckRegistry, ReportRegistry

        reg = CheckRegistry()
        rep_reg = ReportRegistry()

        # Report A: source table does not exist → will error
        rep_reg.register("report_a", {
            "name": "report_a",
            "source": {"type": "duckdb", "path": pipeline_db, "table": "nonexistent_table"},
            "options": {"parallel": False},
            "resolved_checks": [],
            "target_table": "report_a_out",
        })
        # Report B: reads from report_a's output → must be skipped when A errors
        rep_reg.register("report_b", {
            "name": "report_b",
            "source": {"type": "duckdb", "path": pipeline_db, "table": "report_a_out"},
            "options": {"parallel": False},
            "resolved_checks": [],
        })

        results = run_all_reports(
            rep_reg, reg, watermark_store,
            parallel_reports=False,
            pipeline_db=pipeline_db,
        )

        statuses = {r["report"]: r["status"] for r in results}
        assert statuses["report_a"] == "error", "Upstream report must have status=error"
        assert statuses["report_b"] == "skipped", (
            "Dependent report must be skipped (not errored) when upstream errors"
        )

    def test_non_dependent_still_runs_when_sibling_errors(
        self, pipeline_db, watermark_store, sales_df
    ):
        """A report independent of the errored report must still run."""
        from proto_pipe.checks.registry import CheckRegistry, ReportRegistry

        _seed_table(pipeline_db, "sales", sales_df)

        reg = CheckRegistry()
        rep_reg = ReportRegistry()

        # Report A: errors (nonexistent source)
        rep_reg.register("report_a", {
            "name": "report_a",
            "source": {"type": "duckdb", "path": pipeline_db, "table": "nonexistent_table"},
            "options": {"parallel": False},
            "resolved_checks": [],
            "target_table": "report_a_out",
        })
        # Report C: independent — reads from sales, not report_a_out
        rep_reg.register("report_c", {
            "name": "report_c",
            "source": {"type": "duckdb", "path": pipeline_db, "table": "sales",
                       "timestamp_col": "updated_at"},
            "options": {"parallel": False},
            "resolved_checks": [],
        })

        results = run_all_reports(
            rep_reg, reg, watermark_store,
            parallel_reports=False,
            pipeline_db=pipeline_db,
        )

        statuses = {r["report"]: r["status"] for r in results}
        assert statuses["report_a"] == "error"
        assert statuses["report_c"] != "skipped", (
            "Independent report must not be skipped due to an unrelated error"
        )
