from concurrent.futures import ThreadPoolExecutor, as_completed

from proto_pipe.checks.result import CheckResult
from proto_pipe.registry.base import CheckRegistry


def _get_check_args(check_name: str, registry) -> str | None:
    """Extract the column args from a registered check for display in validation_flags.

    Returns a human-readable string like "price", "cost, price", or "cost, price +1"
    representing the column params baked into this check registration.
    Returns None if the check has no column params or isn't found.
    """
    import functools
    import inspect
    from proto_pipe.checks.inspector import CheckParamInspector

    func = registry.get(check_name)
    if func is None:
        return None

    # Get the partial that wrap_series_check wraps (via __wrapped__)
    inner = inspect.unwrap(func)  # follows __wrapped__ → partial or original

    # Get column param names from the original function
    unwrapped = inner
    while isinstance(unwrapped, functools.partial):
        unwrapped = unwrapped.func
    original = inspect.unwrap(unwrapped)
    while isinstance(original, functools.partial):
        original = original.func

    inspector = CheckParamInspector(original)
    col_params = inspector.column_params()
    if not col_params:
        return None

    # Get baked-in column values from the partial's keywords
    if not isinstance(inner, functools.partial):
        return None
    col_values = [
        str(inner.keywords[p])
        for p in col_params
        if p in inner.keywords
    ]
    if not col_values:
        return None

    # Format: "price" / "cost, price" / "cost, price +1"
    MAX_DISPLAY = 2
    if len(col_values) <= MAX_DISPLAY:
        return ", ".join(sorted(col_values))
    return ", ".join(sorted(col_values)[:MAX_DISPLAY]) + f" +{len(col_values) - MAX_DISPLAY}"


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
    {"status": "unavailable", "error": str} # result.available=False
    {"status": "error", "error": str} # exception raised

    :param registry: a registry instance that will run the checks in parallel
    :param check_name: Name of the check that is being run
    :param context: A dictionary containing a `df` key, with a DataFrame value

    """
    try:
        result = registry.run(check_name, context)
        if not isinstance(result, CheckResult):
            return {
                "status": "error",
                "error": f"Check '{check_name}' did not return a CheckResult — got {type(result).__name__}",
            }
        if not result.available:
            return {"status": "unavailable", "error": result.error}
        return {
            "status": "passed" if result.passed else "failed",
            "result": result,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


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

    from proto_pipe.reports.validation_flags import write_validation_flags
    import duckdb
    import pandas as pd

    df: pd.DataFrame = context["df"]
    conn = duckdb.connect(pipeline_db)
    try:
        for check_name, outcome in results.items():
            status = outcome["status"]

            if status in ("error", "unavailable"):
                # Check raised or is unavailable — write one summary flag
                flag_rows = [{"pk_value": None, "reason": outcome.get("error", "")}]

            elif status == "failed":
                # Check ran but found failures — use mask to identify rows
                result: CheckResult = outcome["result"]
                flag_rows = []
                if result.mask is not None:
                    failing = df[result.mask]
                    cols = list(failing.columns)
                    pk_idx = cols.index(pk_col) if pk_col and pk_col in cols else None
                    for row in failing.itertuples(index=False, name=None):
                        pk_value = str(row[pk_idx]) if pk_idx is not None else None
                        flag_rows.append(
                            {
                                "pk_value": pk_value,
                                "reason": result.reason or f"{check_name}: row failed",
                            }
                        )
                else:
                    flag_rows = [{"pk_value": None, "reason": result.reason}]
            else:
                # passed — no flags to write
                continue

            args = _get_check_args(check_name, registry)
            write_validation_flags(
                conn=conn,
                report_name=report_name,
                check_name=check_name,
                table_name=table_name or "",
                pk_col=pk_col,
                flag_rows=flag_rows,
                args=args,
            )
    finally:
        conn.close()

    return results
