"""Validation commands — validate."""

import click

from proto_pipe.io.config import config_path_or_override, load_config


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


@click.command()
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--watermark-db", default=None, help="Override watermark DB path.")
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--table", default=None, help="Run checks for one table only.")
@click.option(
    "--full",
    is_flag=True,
    default=False,
    help="Revalidate all records, ignoring validation_pass state.",
)
def validate(pipeline_db, watermark_db, reports_config, table, full):
    """Run registered checks and transforms against ingested tables.

    Incremental by default — only new or changed records are validated,
    tracked via validation_pass. Use --full to revalidate everything.

    Results and failures are written to validation_block in pipeline.db.
    Failures are warnings — they do not block deliverables.

    \b
    Examples:
      vp validate
      vp validate --table van
      vp validate --full
    """
    from proto_pipe.io.registry import register_from_config
    from proto_pipe.checks.registry import check_registry, report_registry
    from proto_pipe.pipelines.watermark import WatermarkStore
    from proto_pipe.reports.runner import run_all_reports
    from proto_pipe.io.db import write_pipeline_events
    from proto_pipe.checks.helpers import load_custom_checks
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
            r
            for r in report_registry.all()
            if r.get("source", {}).get("table") == table
        ]
        if not reports:
            click.echo(f"[warn] No reports registered for table '{table}'")
            return
    else:
        reports = report_registry.all()

    from proto_pipe.cli.prompts import ValidateProgressReporter

    click.echo(f"\nRunning validation across {len(reports)} report(s)...")

    with ValidateProgressReporter(check_registry) as reporter:
        results = run_all_reports(
            report_registry,
            check_registry,
            watermark_store,
            pipeline_db=p_db,
            full_revalidation=full,
            on_report_done=reporter.on_report_done,
        )

    if table:
        results = [
            r for r in results if r["report"] in {rpt["name"] for rpt in reports}
        ]

    # Write pipeline events — one per completed/errored report
    events = []
    for r in results:
        status = r["status"]
        if status == "error":
            events.append(
                {
                    "event_type": "report_error",
                    "source_name": r["report"],
                    "severity": "error",
                    "detail": r.get("error", ""),
                }
            )
        elif status == "completed":
            has_failures = any(
                v.get("status") in ("failed", "error")
                for v in r.get("results", {}).values()
            )
            events.append(
                {
                    "event_type": (
                        "validation_failed" if has_failures else "validation_passed"
                    ),
                    "source_name": r["report"],
                    "severity": "warn" if has_failures else "info",
                    "detail": "",
                }
            )
        # skipped — no event written
    write_pipeline_events(p_db, events)

    # Summarize validation_block after this run
    conn = duckdb.connect(p_db)
    try:
        total_failures = conn.execute(
            "SELECT count(*) FROM validation_block"
        ).fetchone()[0]

        if total_failures > 0:
            report_count = conn.execute(
                "SELECT count(DISTINCT report_name) FROM validation_block"
            ).fetchone()[0]
            click.echo(
                f"\n⚠  {total_failures} validation failure(s) across "
                f"{report_count} report(s)."
            )
            click.echo("\nTo review and fix:")
            click.echo("  vp validated               — browse failures by report")
            click.echo("  vp validated open <report> — export for external editing")
            click.echo("  vp validated edit --report <report> — edit inline")
        else:
            click.echo("\n✓  No validation failures.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def validation_commands(cli):
    cli.add_command(validate)
