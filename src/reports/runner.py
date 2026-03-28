from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timezone, datetime
from pathlib import Path

import duckdb
import pandas as pd

from src.checks.runner import run_checks, run_checks_and_flag
from src.io.data import load_from_duckdb
from src.io.registry import resolve_filename, write_xlsx_sheet, write_csv
from src.pipelines.watermark import WatermarkStore, _watermark_lock
from src.registry.base import CheckRegistry, ReportRegistry
from src.reports.query import _log_run, init_report_runs_table


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
    check_names = report_config["resolved_checks"]

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
        results = run_checks(check_names, check_registry, context, parallel=parallel_checks)

    # Advance watermark only when every check passed. A failed check means
    # some rows were not fully validated — we want them re-checked next run.
    all_passed = all(v["status"] == "passed" for v in results.values())
    if all_passed:
        ts_col = source_config.get("timestamp_col")
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
