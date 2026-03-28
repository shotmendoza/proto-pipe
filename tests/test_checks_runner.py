"""Tests for src.checks.runner

Covers:
- run_check_safe: returns passed on success
- run_check_safe: returns failed on exception, does not re-raise
- run_check_safe: result dict preserved in outcome
- run_checks: sequential execution returns all results
- run_checks: parallel execution returns all results
- run_checks: failed check does not prevent other checks from running
- run_checks_and_flag: mask mode writes row-level flags to pipeline_db
- run_checks_and_flag: violation_indices mode writes row-level flags
- run_checks_and_flag: raising check writes summary flag
- run_checks_and_flag: summary mode (free-form result) writes one summary flag
- run_checks_and_flag: multiple checks write independent flags
- run_checks_and_flag: idempotent — re-running does not duplicate row-level flags
- run_checks_and_flag: no pipeline_db → no flags written, returns results normally
- run_checks_and_flag: no pk_col → flags written with None pk_value
"""

from functools import partial

import duckdb
import pandas as pd
import pytest

from src.checks.built_in import check_nulls, check_range
from src.checks.runner import run_check_safe, run_checks, run_checks_and_flag
from src.registry.base import CheckRegistry
from src.reports.validation_flags import (
    count_validation_flags,
    detail_df,
    init_validation_flags_table,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def registry():
    return CheckRegistry()


@pytest.fixture()
def pipeline_db(tmp_path):
    db_path = str(tmp_path / "pipeline.db")
    conn = duckdb.connect(db_path)
    init_validation_flags_table(conn)
    conn.close()
    return db_path


@pytest.fixture()
def sample_df():
    return pd.DataFrame({
        "order_id": ["ORD-001", "ORD-002", "ORD-003"],
        "price":    [100.0,    -5.0,      250.0],
        "region":   ["EMEA",   "APAC",    "EMEA"],
    })


@pytest.fixture()
def context(sample_df):
    return {"df": sample_df}


# ---------------------------------------------------------------------------
# run_check_safe
# ---------------------------------------------------------------------------

class TestRunCheckSafe:
    def test_returns_passed_on_success(self, registry, context):
        registry.register("null_check", check_nulls)
        outcome = run_check_safe(registry, "null_check", context)

        assert outcome["status"] == "passed"
        assert "result" in outcome

    def test_result_dict_preserved(self, registry, context):
        registry.register("null_check", check_nulls)
        outcome = run_check_safe(registry, "null_check", context)

        assert "has_nulls" in outcome["result"]

    def test_returns_failed_on_exception(self, registry, context):
        def always_raises(ctx):
            raise ValueError("something went wrong")

        registry.register("bad_check", always_raises)
        outcome = run_check_safe(registry, "bad_check", context)

        assert outcome["status"] == "failed"
        assert "something went wrong" in outcome["error"]

    def test_does_not_re_raise_exception(self, registry, context):
        def always_raises(ctx):
            raise RuntimeError("boom")

        registry.register("boom_check", always_raises)
        # Should not raise
        outcome = run_check_safe(registry, "boom_check", context)
        assert outcome["status"] == "failed"

    def test_unregistered_check_returns_failed(self, registry, context):
        outcome = run_check_safe(registry, "nonexistent_check", context)

        assert outcome["status"] == "failed"
        assert "nonexistent_check" in outcome["error"]


# ---------------------------------------------------------------------------
# run_checks
# ---------------------------------------------------------------------------

class TestRunChecks:
    def test_sequential_runs_all_checks(self, registry, context):
        registry.register("null_check", check_nulls)
        registry.register("range_check", partial(check_range, col="price", min_val=0, max_val=500))

        results = run_checks(["null_check", "range_check"], registry, context, parallel=False)

        assert "null_check" in results
        assert "range_check" in results

    def test_parallel_runs_all_checks(self, registry, context):
        registry.register("null_check", check_nulls)
        registry.register("range_check", partial(check_range, col="price", min_val=0, max_val=500))

        results = run_checks(["null_check", "range_check"], registry, context, parallel=True)

        assert "null_check" in results
        assert "range_check" in results

    def test_parallel_and_sequential_produce_same_statuses(self, registry, context):
        registry.register("null_check", check_nulls)
        registry.register("range_check", partial(check_range, col="price", min_val=0, max_val=500))

        names = ["null_check", "range_check"]
        seq = run_checks(names, registry, context, parallel=False)
        par = run_checks(names, registry, context, parallel=True)

        assert {k: v["status"] for k, v in seq.items()} == {k: v["status"] for k, v in par.items()}

    def test_one_failed_check_does_not_prevent_others(self, registry, context):
        def always_raises(ctx):
            raise ValueError("fail")

        registry.register("bad_check", always_raises)
        registry.register("null_check", check_nulls)

        results = run_checks(["bad_check", "null_check"], registry, context)

        assert results["bad_check"]["status"] == "failed"
        assert results["null_check"]["status"] == "passed"

    def test_empty_check_list_returns_empty_dict(self, registry, context):
        results = run_checks([], registry, context)
        assert results == {}


# ---------------------------------------------------------------------------
# run_checks_and_flag
# ---------------------------------------------------------------------------

class TestRunChecksAndFlag:
    def test_mask_mode_writes_row_level_flag(self, registry, pipeline_db, sample_df):
        def check_negative_price(ctx, col="price"):
            df = ctx["df"]
            return {"mask": df[col] < 0, "flag_when": True}

        registry.register("neg_price", check_negative_price)

        run_checks_and_flag(
            check_names=["neg_price"],
            registry=registry,
            context={"df": sample_df},
            pipeline_db=pipeline_db,
            report_name="sales_report",
            table_name="sales",
            pk_col="order_id",
        )

        conn = duckdb.connect(pipeline_db)
        try:
            det = detail_df(conn, "sales_report")
            assert len(det) == 1
            assert det.iloc[0]["pk_value"] == "ORD-002"
            assert "neg_price" in det.iloc[0]["reason"]
        finally:
            conn.close()

    def test_violation_indices_mode_writes_row_level_flag(self, registry, pipeline_db, sample_df):
        registry.register("price_range", partial(check_range, col="price", min_val=0, max_val=500))

        run_checks_and_flag(
            check_names=["price_range"],
            registry=registry,
            context={"df": sample_df},
            pipeline_db=pipeline_db,
            report_name="sales_report",
            table_name="sales",
            pk_col="order_id",
        )

        conn = duckdb.connect(pipeline_db)
        try:
            det = detail_df(conn, "sales_report")
            assert len(det) == 1
            assert det.iloc[0]["pk_value"] == "ORD-002"
            assert "-5.0" in det.iloc[0]["reason"]
        finally:
            conn.close()

    def test_raising_check_writes_summary_flag(self, registry, pipeline_db, sample_df):
        def always_raises(ctx):
            raise ValueError("completely broken")

        registry.register("broken_check", always_raises)

        run_checks_and_flag(
            check_names=["broken_check"],
            registry=registry,
            context={"df": sample_df},
            pipeline_db=pipeline_db,
            report_name="sales_report",
            table_name="sales",
            pk_col="order_id",
        )

        conn = duckdb.connect(pipeline_db)
        try:
            det = detail_df(conn, "sales_report")
            assert len(det) == 1
            assert det.iloc[0]["pk_value"] is None
            assert "completely broken" in det.iloc[0]["reason"]
        finally:
            conn.close()

    def test_summary_mode_free_form_writes_one_flag(self, registry, pipeline_db, sample_df):
        registry.register("null_check", check_nulls)

        run_checks_and_flag(
            check_names=["null_check"],
            registry=registry,
            context={"df": sample_df},
            pipeline_db=pipeline_db,
            report_name="sales_report",
            table_name="sales",
            pk_col="order_id",
        )

        conn = duckdb.connect(pipeline_db)
        try:
            # null_check returns a free-form dict → one summary flag
            assert count_validation_flags(conn, "sales_report") == 1
            assert detail_df(conn).iloc[0]["pk_value"] is None
        finally:
            conn.close()

    def test_multiple_checks_write_independent_flags(self, registry, pipeline_db, sample_df):
        def check_negative(ctx, col="price"):
            df = ctx["df"]
            return {"mask": df[col] < 0}

        registry.register("neg_price", check_negative)
        registry.register("null_check", check_nulls)

        run_checks_and_flag(
            check_names=["neg_price", "null_check"],
            registry=registry,
            context={"df": sample_df},
            pipeline_db=pipeline_db,
            report_name="sales_report",
            table_name="sales",
            pk_col="order_id",
        )

        conn = duckdb.connect(pipeline_db)
        try:
            # neg_price: 1 row-level flag; null_check: 1 summary flag
            assert count_validation_flags(conn, "sales_report") == 2
        finally:
            conn.close()

    def test_idempotent_row_level_flags_not_duplicated(self, registry, pipeline_db, sample_df):
        registry.register("price_range", partial(check_range, col="price", min_val=0, max_val=500))
        ctx = {"df": sample_df}

        run_checks_and_flag(
            check_names=["price_range"], registry=registry, context=ctx,
            pipeline_db=pipeline_db, report_name="r", table_name="sales", pk_col="order_id",
        )
        run_checks_and_flag(
            check_names=["price_range"], registry=registry, context=ctx,
            pipeline_db=pipeline_db, report_name="r", table_name="sales", pk_col="order_id",
        )

        conn = duckdb.connect(pipeline_db)
        try:
            # uuid5 dedup: same (report, check, pk_value) → same id → only 1 flag
            assert count_validation_flags(conn, "r") == 1
        finally:
            conn.close()

    def test_no_pipeline_db_returns_results_without_writing_flags(self, registry, sample_df):
        registry.register("null_check", check_nulls)

        results = run_checks_and_flag(
            check_names=["null_check"],
            registry=registry,
            context={"df": sample_df},
            pipeline_db=None,
            report_name=None,
        )

        assert results["null_check"]["status"] == "passed"
        # No DB → nothing to assert on flags; test confirms no exception raised

    def test_no_pk_col_flags_written_with_none_pk_value(self, registry, pipeline_db, sample_df):
        def check_negative(ctx, col="price"):
            df = ctx["df"]
            return {"mask": df[col] < 0}

        registry.register("neg_price", check_negative)

        run_checks_and_flag(
            check_names=["neg_price"],
            registry=registry,
            context={"df": sample_df},
            pipeline_db=pipeline_db,
            report_name="r",
            table_name="sales",
            pk_col=None,  # no primary key defined
        )

        conn = duckdb.connect(pipeline_db)
        try:
            det = detail_df(conn, "r")
            assert len(det) == 1
            assert det.iloc[0]["pk_value"] is None
        finally:
            conn.close()

    def test_returns_same_structure_as_run_checks(self, registry, pipeline_db, sample_df):
        registry.register("null_check", check_nulls)

        results = run_checks_and_flag(
            check_names=["null_check"],
            registry=registry,
            context={"df": sample_df},
            pipeline_db=pipeline_db,
            report_name="r",
            table_name="sales",
            pk_col="order_id",
        )

        assert "null_check" in results
        assert results["null_check"]["status"] in ("passed", "failed")