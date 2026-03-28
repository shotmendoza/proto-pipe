"""Validation commands — validate, checks."""

import click

from .helpers import config_path_or_override, load_custom_checks
from ..reports.validation_flags import count_validation_flags, summary_df

# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


@click.command()
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--watermark-db", default=None, help="Override watermark DB path.")
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--table", default=None, help="Run checks for one table only.")
def validate(pipeline_db, watermark_db, reports_config, table):
    """Run all registered checks against ingested tables.

    :param pipeline_db: Overrides the default pipeline database path if provided
    :param watermark_db: Overrides the default watermark database path if provided
    :param reports_config: Overrides the default reports configuration file path if provided
    :param table: Runs checks and generates reports for a specific table, if specified
    :return: None

    Checks are watermark-filtered — only rows newer than the last successful
    run are evaluated. Results and flagged rows are written to pipeline.db.

    \b
    Example:
      vp validate
      vp validate --table sales
    """
    from src.io.registry import load_config, register_from_config
    from src.registry.base import check_registry, report_registry
    from src.pipelines.watermark import WatermarkStore
    from src.reports.runner import run_all_reports

    import duckdb

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    w_db = config_path_or_override("watermark_db", watermark_db)
    rep_cfg = config_path_or_override("reports_config", reports_config)

    load_custom_checks(check_registry)
    _config = load_config(rep_cfg)
    register_from_config(_config, check_registry, report_registry)
    watermark_store = WatermarkStore(w_db)

    if table:
        reports = [
            r for r in report_registry.all()
            if r.get("source", {}).get("table") == table
        ]
        if not reports:
            click.echo(f"  [warn] No reports registered for table '{table}'")
            return
    else:
        reports = report_registry.all()

    click.echo(f"\nRunning validation across {len(reports)} report(s)...")
    results = run_all_reports(
        report_registry,
        check_registry,
        watermark_store,
        pipeline_db=p_db
    )

    for r in results:
        status = r["status"]
        click.echo(f"\n  {r['report']} [{status}]")
        if status == "completed":
            for check_name, outcome in r["results"].items():
                mark = "✓" if outcome["status"] == "passed" else "✗"
                click.echo(f"{mark} {check_name}")
                if outcome["status"] == "failed":
                    click.echo(f"{outcome['error']}")

        # Summarise validation flags written during this run
    conn = duckdb.connect(p_db)
    try:
        total_flags = count_validation_flags(conn)
        if total_flags > 0:
            summ = summary_df(conn)
            click.echo(
                f"\n  ⚠  {total_flags} validation flag(s) across {summ['report_name'].nunique()} report(s)."
            )
            click.echo(
                "  Run: vp export-validation  to export a detail + summary report."
            )
        else:
            click.echo("\n  ✓  No validation flags.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# checks
# ---------------------------------------------------------------------------
@click.command()
def checks():
    """List all available built-in checks and their parameters.

    \b
    Example:
      vp checks
    """
    from src.checks.built_in import BUILT_IN_CHECKS

    descriptions = {
        "null_check": ("No params required", "Checks all columns for null values"),
        "range_check": ("col, min_val, max_val", "Checks a column's values fall within a range"),
        "schema_check": ("expected_cols (list)", "Checks the table has the expected columns"),
        "duplicate_check": ("subset (list, optional)", "Checks for duplicate rows"),
    }

    click.echo("\nBuilt-in checks:\n")
    for name in BUILT_IN_CHECKS:
        params, desc = descriptions.get(name, ("", ""))
        click.echo(f"{name}")
        click.echo(f"params: {params}")
        click.echo(f"{desc}\n")


# ---------------------------------------------------------------------------
# export-validation
# ---------------------------------------------------------------------------


@click.command("export-validation")
@click.option(
    "--report", default=None, help="Export flags for one report only. Omit for all."
)
@click.option(
    "--output",
    default=None,
    help="Output path. Defaults to validation_<report>_<date>.xlsx in output_dir.",
)
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def export_validation(report, output, pipeline_db):
    """Export validation flags to a two-sheet Excel file.

    Sheet 'Detail' — one row per flagged record, with the source record's
                       ID column named after the actual primary key field.
    Sheet 'Summary' — one row per check, with total failure counts.

    Validation flags are warnings — they do not block deliverables. Use this
    report to identify which records need to be corrected at the source,
    then re-ingest and re-validate.

    \b
    Examples:
      vp export-validation
      vp export-validation --report daily_sales_validation
      vp export-validation --output /tmp/sales_validation.xlsx
    """
    from datetime import date
    from pathlib import Path
    import duckdb

    from src.reports.validation_flags import export_validation_report

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    out_dir = config_path_or_override("output_dir")

    if output:
        output_path = output
    else:
        today = date.today().isoformat()
        stem = f"validation_{report}_{today}" if report else f"validation_{today}"
        output_path = str(Path(out_dir) / f"{stem}.xlsx")

    conn = duckdb.connect(p_db)
    try:
        detail_rows, summary_rows = export_validation_report(conn, output_path, report)
        click.echo(f"[ok] {detail_rows} flagged record(s) exported to: {output_path}")
        click.echo(
            f"Detail: {detail_rows} row(s)  |  Summary: {summary_rows} check(s)"
        )
        click.echo(
            f"\nR"
            f"eview the Detail sheet to identify which records need correction."
        )
        click.echo(f"Fix them at the source, re-ingest, and re-validate.")
    except ValueError as e:
        click.echo(f"[error] {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
def validation_commands(cli):
    cli.add_command(validate)
    cli.add_command(checks)
    cli.add_command(export_validation)
