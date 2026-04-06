from concurrent.futures import ThreadPoolExecutor, as_completed

from proto_pipe.checks.result import CheckResult, CheckOutcome
from proto_pipe.checks.registry import CheckRegistry, CheckParamInspector
from proto_pipe.pipelines.flagging import FlagRecord, write_validation_flags
from proto_pipe.io.db import flag_id_for


def _get_check_args(check_name: str, registry) -> str | None:
    """Extract the column args from a registered check for display.

    Returns a human-readable string like "price", "cost, price", or "cost, price +1"
    representing the column params baked into this check registration.
    Returns None if the check has no column params or isn't found.
    """
    import functools
    import inspect

    func = registry.get(check_name)
    if func is None:
        return None

    inner = inspect.unwrap(func)

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

    if not isinstance(inner, functools.partial):
        return None
    col_values = [
        str(inner.keywords[p])
        for p in col_params
        if p in inner.keywords
    ]
    if not col_values:
        return None

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
    context: dict,
) -> CheckOutcome:
    """Run a single check, catching exceptions so one failure doesn't halt the report.

    Returns:
      CheckOutcome(status="passed", result=<CheckResult>)
      CheckOutcome(status="failed", result=<CheckResult>)
      CheckOutcome(status="unavailable", error=str)
      CheckOutcome(status="error", error=str)
    """
    try:
        result = registry.run(check_name, context)
        if not isinstance(result, CheckResult):
            return CheckOutcome(
                status="error",
                error=(
                    f"Check '{check_name}' did not return a CheckResult "
                    f". Got {type(result).__name__}"
                ),
            )
        if not result.available:
            return CheckOutcome(status="unavailable", error=result.error)
        return CheckOutcome(
            status="passed" if result.passed else "failed",
            result=result,
        )
    except Exception as e:
        return CheckOutcome(status="error", error=str(e))


def run_checks(
    check_names: list[str],
    registry: CheckRegistry,
    context: dict,
    parallel: bool = False,
) -> dict[str, CheckOutcome]:
    """Run checks sequentially or in parallel.

    :param check_names: Names of registered checks to run.
    :param registry:    CheckRegistry instance.
    :param context:     Dict with "df" key (the DataFrame to check against).
    :param parallel:    Run checks in threads if True.
    :return:            {check_name: CheckOutcome}
    """
    if not parallel:
        return {name: run_check_safe(registry, name, context) for name in check_names}

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
) -> dict[str, CheckOutcome]:
    """Run checks and write per-row validation failures to validation_block.

    Wraps run_checks. After all checks complete, inspects each result and
    writes FlagRecord entries to validation_block for any check that produced
    failing rows or a summary failure.

    When pipeline_db / report_name are not provided, behaves identically to
    run_checks — no flags are written. Safe to call in tests without a DB.

    :param check_names:  Names of registered checks to run.
    :param registry:     CheckRegistry instance.
    :param context:      Dict with "df" key (the DataFrame to check against).
    :param parallel:     Run checks in threads if True.
    :param pipeline_db:  Path to pipeline.db. Required for flag writing.
    :param report_name:  Report name — written to validation_block.report_name.
    :param table_name:   Source/report table name.
    :param pk_col:       Primary key column for row identity, or None.
    :return:             {check_name: CheckOutcome}
    """
    results = run_checks(check_names, registry, context, parallel=parallel)

    if not pipeline_db or not report_name:
        return results

    import duckdb
    import pandas as pd

    df: pd.DataFrame = context["df"]
    conn = duckdb.connect(pipeline_db)
    try:
        for check_name, outcome in results.items():
            status = outcome.status
            args = _get_check_args(check_name, registry)
            # args (e.g. "price" / "cost, price") written to bad_columns —
            # validation_block has no dedicated args column
            bad_columns = args

            if status in ("error", "unavailable"):
                flag_records = [
                    FlagRecord(
                        id=flag_id_for(None),
                        table_name=table_name or "",
                        report_name=report_name,
                        check_name=check_name,
                        pk_value=None,
                        bad_columns=bad_columns,
                        reason=(outcome.error or "")[:500],
                    )
                ]

            elif status == "failed":
                result: CheckResult = outcome.result
                flag_records = []
                if result.mask is not None:
                    failing = df[result.mask]
                    pk_idx = (
                        list(failing.columns).index(pk_col)
                        if pk_col and pk_col in failing.columns
                        else None
                    )
                    for row in failing.itertuples(index=False, name=None):
                        pk_value = str(row[pk_idx]) if pk_idx is not None else None
                        flag_records.append(
                            FlagRecord(
                                id=flag_id_for(pk_value),
                                table_name=table_name or "",
                                report_name=report_name,
                                check_name=check_name,
                                pk_value=pk_value,
                                bad_columns=bad_columns,
                                reason=(result.reason or f"{check_name}: row failed")[
                                    :500
                                ],
                            )
                        )
                else:
                    flag_records = [
                        FlagRecord(
                            id=flag_id_for(None),
                            table_name=table_name or "",
                            report_name=report_name,
                            check_name=check_name,
                            pk_value=None,
                            bad_columns=bad_columns,
                            reason=(result.reason or "")[:500],
                        )
                    ]
            else:
                continue

            write_validation_flags(conn, flag_records)

    finally:
        conn.close()

    return results
