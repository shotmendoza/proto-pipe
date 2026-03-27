"""Validation commands — validate, checks."""

import click

from .helpers import config_path_or_override, load_custom_checks


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


@click.command()
@click.option("--pipeline-db",    default=None, help="Override pipeline DB path.")
@click.option("--watermark-db",   default=None, help="Override watermark DB path.")
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--table",          default=None, help="Run checks for one table only.")
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
    results = run_all_reports(report_registry, check_registry, watermark_store)

    for r in results:
        status = r["status"]
        click.echo(f"\n  {r['report']} [{status}]")
        if status == "completed":
            for check_name, outcome in r["results"].items():
                mark = "✓" if outcome["status"] == "passed" else "✗"
                click.echo(f"{mark} {check_name}")
                if outcome["status"] == "failed":
                    click.echo(f"{outcome['error']}")


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
# Registration
# ---------------------------------------------------------------------------
def validation_commands(cli):
    cli.add_command(validate)
    cli.add_command(checks)
