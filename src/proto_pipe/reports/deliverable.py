"""Deliverable runner — output file generation.

Moved from reports/runner.py per REFACTOR_PLAN.md. Produces output files
(CSV or Excel) from report DataFrames and logs each run to report_runs.

No print() — uses ReportCallback.on_deliverable_written() for output
notification. The callback parameter is optional; callers that don't
need progress output (tests, batch scripts) omit it.

No CLI imports — reports/ never imports click, rich, or questionary.
"""
from __future__ import annotations

from datetime import timezone, datetime
from pathlib import Path

import duckdb
import pandas as pd

from proto_pipe.io.registry import resolve_filename, write_xlsx_sheet, write_csv
from proto_pipe.pipelines.query import log_run, init_report_runs_table
from proto_pipe.reports.callbacks import ReportCallback


def run_deliverable(
    deliverable: dict,
    report_dataframes: dict[str, pd.DataFrame],
    output_dir: str,
    pipeline_db_path: str,
    run_date: str | None = None,
    callback: ReportCallback | None = None,
) -> list[str]:
    """Write output files for a deliverable and log each report run."""
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
        sheets = {}
        filename = resolve_filename(template, name, run_date)
        output_path = out_dir / filename

        for report_cfg in reports:
            report_name = report_cfg["name"]
            sheet = report_cfg.get("sheet", report_name)
            df = report_dataframes.get(report_name, pd.DataFrame())
            sheets[sheet] = df
            log_run(
                conn, name, report_name, filename, str(out_dir),
                report_cfg.get("filters"), len(df), fmt, run_date,
            )

        write_xlsx_sheet(sheets, output_path)
        written.append(str(output_path))
        total_rows = sum(len(d) for d in sheets.values())
        if callback:
            callback.on_deliverable_written(str(output_path), total_rows)

    else:
        for report_cfg in reports:
            report_name = report_cfg["name"]
            filename = resolve_filename(template, report_name, run_date)
            output_path = out_dir / filename
            df = report_dataframes.get(report_name, pd.DataFrame())

            write_csv(df, output_path)
            log_run(
                conn, name, report_name, filename, str(out_dir),
                report_cfg.get("filters"), len(df), fmt, run_date,
            )
            written.append(str(output_path))
            if callback:
                callback.on_deliverable_written(str(output_path), len(df))

    conn.close()
    return written
