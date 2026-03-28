from concurrent.futures import ThreadPoolExecutor, as_completed

from src.proto_pipe.registry.base import CheckRegistry


# ---------------------------------------------------------------------------
# Check execution
# ---------------------------------------------------------------------------
def run_check_safe(
        registry: CheckRegistry,
        check_name: str,
        context: dict
) -> dict:
    """Run a single check, catching exceptions so one failure doesn't halt the report.

    Returns:
    {"status": "passed", "result": <dict>}
    {"status": "failed", "error": <str>}

    :param registry: a registry instance that will run the checks in parallel
    :param check_name: Name of the check that is being run
    :param context: A dictionary containing a `df` key, with a DataFrame value

    """
    try:
        result = registry.run(check_name, context)
        return {"status": "passed", "result": result}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


def run_checks(
    check_names: list[str],
    registry: CheckRegistry,
    context: dict,
    parallel: bool = False,
) -> dict:
    """manager object that determines whether the checks should be run in parallel or thread safe.

    :param check_names: name of the checks in the registry
    :param registry: where all the checks are logged
    :param context: a dictionary containing a `df` key, with a DataFrame value
    :param parallel: whether to run the checks in parallel
    :return: the results of the checks
    """
    if not parallel:
        return {
            name: run_check_safe(registry, name, context)
            for name in check_names
        }

    results = {}
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(run_check_safe, registry, name, context): name
            for name in check_names
        }
        for future in as_completed(futures):
            name = futures[future]
            results[name] = future.result()
    return results


# ---------------------------------------------------------------------------
# Batch execution with validation flag writing
# ---------------------------------------------------------------------------


def run_checks_and_flag(
    check_names: list[str],
    registry: CheckRegistry,
    context: dict,
    parallel: bool = False,
    pipeline_db: str | None = None,
    report_name: str | None = None,
    table_name: str | None = None,
    pk_col: str | None = None,
) -> dict:
    """Run checks and write per-row validation flags to pipeline.db.

    Wraps run_checks. After all checks complete, inspects each result and
    writes entries to the validation_flags table for any check that produced
    identifiable failing rows (mask or violation_indices) or a summary failure.

    When pipeline_db / report_name are not provided, behaves identically to
    run_checks — no flags are written. Safe to call in tests without a DB.

    Args:
        check_names:  Names of registered checks to run.
        registry:     CheckRegistry instance.
        context:      Dict with "df" key (the watermark-filtered DataFrame).
        parallel:     Run checks in threads if True.
        pipeline_db:  Path to pipeline.db. Required for flag writing.
        report_name:  Name of the report being validated.
        table_name:   Source table name.
        pk_col:       Primary key column name for the source, or None.

    Returns:
        Dict of {check_name: outcome} — same shape as run_checks.
    """
    results = run_checks(check_names, registry, context, parallel=parallel)

    # Skip flag writing if not configured
    if not pipeline_db or not report_name:
        return results

    from src.proto_pipe.reports.validation_flags import (
        _extract_flagged_rows,
        write_validation_flags,
    )
    import duckdb
    import pandas as pd
    from typing import Any

    df: pd.DataFrame = context["df"]
    conn = duckdb.connect(pipeline_db)
    try:
        for check_name, outcome in results.items():
            if outcome["status"] == "failed":
                # Check raised — write one summary flag
                flag_rows = [{"pk_value": None, "reason": outcome["error"]}]
            else:
                result_dict: dict[str, Any] = outcome.get("result", {})
                flag_rows = _extract_flagged_rows(
                    result=result_dict,
                    df=df,
                    pk_col=pk_col,
                    check_name=check_name,
                )

            write_validation_flags(
                conn=conn,
                report_name=report_name,
                check_name=check_name,
                table_name=table_name or "",
                pk_col=pk_col,
                flag_rows=flag_rows,
            )
    finally:
        conn.close()

    return results
