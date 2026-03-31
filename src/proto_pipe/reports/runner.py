from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timezone, datetime
from pathlib import Path

import duckdb
import pandas as pd

from proto_pipe.checks.runner import run_checks, run_checks_and_flag
from proto_pipe.io.data import load_from_duckdb
from proto_pipe.io.registry import resolve_filename, write_xlsx_sheet, write_csv
from proto_pipe.pipelines.watermark import WatermarkStore, _watermark_lock
from proto_pipe.registry.base import CheckRegistry, ReportRegistry
from proto_pipe.reports.query import _log_run, init_report_runs_table


# ---------------------------------------------------------------------------
# Transform application
# ---------------------------------------------------------------------------
def _apply_transforms(
    transform_names: list[str],
    check_registry: CheckRegistry,
    df: pd.DataFrame,
    table_name: str,
    pipeline_db: str,
) -> None:
    """Apply transforms in order, then write the result back to DuckDB in one pass.

    Each transform receives {"df": working_df} and must return either:
      - pd.DataFrame — replaces the working DataFrame entirely
      - pd.Series    — replaces a single column (series.name must match a column)

    On exception, that transform is skipped and the working DataFrame is left
    unchanged for subsequent transforms. A single DuckDB write happens after
    all transforms complete.

    :param transform_names: Ordered list of transform names from the registry.
    :param check_registry:  Registry holding the transform callables.
    :param df:              The current report DataFrame.
    :param table_name:      DuckDB table to write the final result back to.
    :param pipeline_db:     Path to the pipeline DuckDB file.
    """
    working_df = df.copy()
    failed: list[str] = []

    for name in transform_names:
        try:
            result = check_registry.run(name, {"df": working_df})
            if isinstance(result, pd.DataFrame):
                working_df = result
            elif isinstance(result, pd.Series):
                col_name = result.name
                if col_name and col_name in working_df.columns:
                    working_df = working_df.copy()
                    working_df[col_name] = result.values
                else:
                    print(
                        f"  [transform-warn] '{name}' returned a Series with "
                        f"name={col_name!r} — not found in table columns, skipped"
                    )
            else:
                print(
                    f"  [transform-warn] '{name}' returned {type(result).__name__} "
                    f"(expected DataFrame or Series) — skipped"
                )
        except Exception as e:
            print(f"  [transform-fail] '{name}': {e} — skipped, table unchanged")
            failed.append(name)

    # Single write back — one DuckDB round-trip regardless of how many transforms ran.
    conn = duckdb.connect(pipeline_db)
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM working_df')
    finally:
        conn.close()

    if failed:
        print(
            f"  [transform] {len(transform_names) - len(failed)}/{len(transform_names)} "
            f"transform(s) applied to '{table_name}'. Failed: {', '.join(failed)}"
        )
    else:
        print(
            f"  [transform] {len(transform_names)} transform(s) applied to '{table_name}'"
        )


# ---------------------------------------------------------------------------
# Report execution
# ---------------------------------------------------------------------------
def run_report(
    report_config: dict,
    check_registry: CheckRegistry,
    watermark_store: WatermarkStore,
    pipeline_db: str | None = None,
) -> dict:
    """Executes a full report process, involving data loading, checks resolution,
    execution of checks, and watermark management for persisted state tracking.

    :param pipeline_db: Path to pipeline.db. When provided, validation flags
                         are written for each check result. Optional — omit in
                         tests that don't need flag writing.
    :param check_registry:
    :param report_config: Configuration for the report execution. Contains the report name,
        source information, options, and defined checks.

    :param watermark_store: Storage interface for managing watermarks, which track
        the last processed timestamp for data sources.

    :return: A dictionary with the executed report details, including the name of
        the report, its final status, and the results of any checks run.

    Returns:
        {
            "report": str,
            "status": "completed" | "skipped" | "error",
            "results": {check_name: outcome, ...}, # present when completed
            "error": str, # present when status=error
        }
    """
    report_name = report_config["name"]
    source_config = report_config["source"]
    options = report_config.get("options", {})
    parallel_checks = options.get("parallel", False)

    pk_col = source_config.get("primary_key")
    table_name = source_config.get("table", "")

    last_run_at = watermark_store.get(report_name)

    try:
        df = load_from_duckdb(source_config, last_run_at)
    except Exception as e:
        return {"report": report_name, "status": "error", "error": str(e)}

    if df.empty:
        return {"report": report_name, "status": "skipped"}

    context = {"df": df}

    # Split resolved names by kind — checks run first, then transforms.
    all_names = report_config["resolved_checks"]
    check_names = [n for n in all_names if check_registry.get_kind(n) == "check"]
    transform_names = [
        n for n in all_names if check_registry.get_kind(n) == "transform"
    ]

    if pipeline_db:
        results = run_checks_and_flag(
            check_names=check_names,
            registry=check_registry,
            context=context,
            parallel=parallel_checks,
            pipeline_db=pipeline_db,
            report_name=report_name,
            table_name=table_name,
            pk_col=pk_col,
        )
    else:
        results = run_checks(
            check_names, check_registry, context, parallel=parallel_checks
        )

    # Apply transforms after checks so validation runs against unmodified data.
    if transform_names and pipeline_db:
        _apply_transforms(transform_names, check_registry, df, table_name, pipeline_db)

    # Advance watermark only when every check passed. A failed check means
    # some rows were not fully validated — we want them re-checked next run.
    all_passed = all(v["status"] == "passed" for v in results.values())
    if all_passed:
        ts_col = source_config.get("timestamp_col") or "_ingested_at"
        if ts_col and ts_col in df.columns:
            max_ts = df[ts_col].max()
            if not isinstance(max_ts, datetime):
                max_ts = datetime.fromisoformat(str(max_ts)).replace(
                    tzinfo=timezone.utc
                )
            watermark_store.set(report_name, max_ts)
    return {"report": report_name, "status": "completed", "results": results}


def _advance_watermark(
        report_name: str,
        timestamp_col: str,
        df: pd.DataFrame,
        watermark_store: WatermarkStore,
) -> None:
    """Advance the report watermark to the latest timestamp in the loaded data."""
    ts_series = pd.to_datetime(df[timestamp_col], utc=True)
    last_processed_ts = ts_series.max().to_pydatetime()
    with _watermark_lock:
        watermark_store.set(report_name, last_processed_ts)


# ---------------------------------------------------------------------------
# Main deliverable runner
# ---------------------------------------------------------------------------
def run_deliverable(
    deliverable: dict,
    report_dataframes: dict[str, pd.DataFrame],  # report_name -> DataFrame
    output_dir: str,
    pipeline_db_path: str,
    cli_overrides: dict | None = None,  # { report_name: filters }
    run_date: str | None = None,
) -> list[str]:
    """
    Write output files for a deliverable and log each report run.

    Returns list of output file paths written.
    """

    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fmt = deliverable.get("format", "csv")
    name = deliverable["name"]
    template = deliverable["filename_template"]
    out_dir = Path(deliverable.get("output_dir", output_dir))
    reports = deliverable["reports"]

    conn = duckdb.connect(pipeline_db_path)
    init_report_runs_table(conn)

    written = []

    if fmt == "xlsx":
        # All reports go into one Excel file as separate sheets
        sheets = {}
        filename = resolve_filename(template, name, run_date)
        output_path = out_dir / filename

        for report_cfg in reports:
            report_name = report_cfg["name"]
            sheet = report_cfg.get("sheet", report_name)
            df = report_dataframes.get(report_name, pd.DataFrame())
            sheets[sheet] = df

            _log_run(conn, name, report_name, filename, str(out_dir),
                     report_cfg.get("filters"), len(df), fmt, run_date)

        write_xlsx_sheet(sheets, output_path)
        written.append(str(output_path))
        print(f"  [ok] {output_path} ({sum(len(d) for d in sheets.values())} total rows)")

    else:
        # One CSV per report
        for report_cfg in reports:
            report_name = report_cfg["name"]
            filename = resolve_filename(template, report_name, run_date)
            output_path = out_dir / filename
            df = report_dataframes.get(report_name, pd.DataFrame())

            write_csv(df, output_path)
            _log_run(conn, name, report_name, filename, str(out_dir),
                     report_cfg.get("filters"), len(df), fmt, run_date)

            written.append(str(output_path))
            print(f"  [ok] {output_path} ({len(df)} rows)")

    conn.close()
    return written


# ---------------------------------------------------------------------------
# Top-level runner
# ---------------------------------------------------------------------------
def run_all_reports(
    report_registry: ReportRegistry,
    check_registry: CheckRegistry,
    watermark_store: WatermarkStore,
    parallel_reports: bool = True,
    pipeline_db: str | None = None,
) -> list[dict]:
    """Execute all reports based on the inputs provided and returns their results. The function
    supports both sequential and parallel execution of reports, depending on the
    `parallel_reports` flag.

    :param pipeline_db: Path to pipeline.db. Passed through to run_report
                          for validation flag writing
    :param report_registry: The registry contains all the report definitions.
    :param check_registry: The registry holding all the checks to be executed for the reports.
    :param watermark_store: The storage handling watermarking for report processing.
    :param parallel_reports: Flag indicating whether to execute reports in parallel (default is True).
    :return: A list of dictionaries storing the results of each executed report.
    """
    reports = report_registry.all()

    if not parallel_reports:
        return [
            run_report(r, check_registry, watermark_store, pipeline_db=pipeline_db)
            for r in reports
        ]

    results = []
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(
                run_report,
                r,
                check_registry,
                watermark_store,
                pipeline_db=pipeline_db,
            ): r["name"] for r in reports
        }
        for future in as_completed(futures):
            results.append(future.result())
    return results
