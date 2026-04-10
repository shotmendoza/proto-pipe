"""Status commands — vp status, vp status source, vp status report, vp status deliverable.

Primary inspection command. Shows pipeline health with prescriptive next steps.
"""
from __future__ import annotations

import click
import duckdb

from proto_pipe.io.config import config_path_or_override


# Reuse StageGroup from errors.py
from proto_pipe.cli.errors import StageGroup


# ---------------------------------------------------------------------------
# vp status (top-level)
# ---------------------------------------------------------------------------

@click.group(
    "status",
    cls=StageGroup,
    invoke_without_command=True,
    context_settings={"max_content_width": 120},
)
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option(
    "--log",
    "show_log",
    is_flag=True,
    default=False,
    help="Show pipeline_events log instead of health summary.",
)
@click.option(
    "--export",
    "export_log",
    is_flag=True,
    default=False,
    help="Export pipeline_events to CSV (use with --log).",
)
@click.option(
    "--severity",
    default=None,
    type=click.Choice(["info", "warn", "error"]),
    help="Filter log events by severity (use with --log).",
)
@click.option(
    "--since",
    default=None,
    help="Filter log events on or after this date, YYYY-MM-DD (use with --log).",
)
@click.option(
    "--clear",
    is_flag=True,
    default=False,
    help="Delete exported rows from pipeline_events (use with --log --export).",
)
@click.pass_context
def status_cmd(ctx, pipeline_db, show_log, export_log, severity, since, clear):
    """Pipeline health summary with prescriptive next steps.

    \b
    Examples:
      vp status                         — health overview
      vp status source                  — all sources, one line each
      vp status source van              — one source in detail
      vp status report                  — all reports, one line each
      vp status --log                   — show pipeline events
      vp status --log --severity error  — show only errors
      vp status --log --export          — export events to CSV
    """
    if ctx.invoked_subcommand is not None:
        return

    p_db = config_path_or_override("pipeline_db", pipeline_db)

    if show_log:
        _handle_log(p_db, severity, since, export_log, clear)
        return

    from proto_pipe.pipelines.query import query_pipeline_health
    from proto_pipe.cli.prompts import print_health_summary

    conn = duckdb.connect(p_db)
    try:
        health = query_pipeline_health(conn)
    finally:
        conn.close()

    try:
        from proto_pipe.io.config import DeliverableConfig
        del_cfg = config_path_or_override("deliverables_config")
        del_names = DeliverableConfig(del_cfg).names()
    except Exception:
        del_names = []

    print_health_summary(health, del_names)


def _handle_log(p_db, severity, since, export_log, clear):
    """Handle --log mode: display or export pipeline_events."""
    from datetime import date, datetime, timezone
    from pathlib import Path
    from proto_pipe.pipelines.query import query_pipeline_events

    conn = duckdb.connect(p_db)
    try:
        try:
            df = query_pipeline_events(conn, severity, since, order_desc=not export_log)
        except ValueError:
            click.echo(f"[error] --since must be in YYYY-MM-DD format, got: {since}")
            return

        if df.empty:
            click.echo("[info] No events matched the given filters.")
            return

        # Strip timezone for display/CSV compat
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].dt.tz_localize(None)

        if export_log:
            log_dir = config_path_or_override("log_dir")
            today = date.today().isoformat()
            output_path = str(Path(log_dir) / f"pipeline_events_{today}.csv")
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
            click.echo(f"[ok] {len(df)} event(s) exported to: {output_path}")

            if clear:
                delete_query = "DELETE FROM pipeline_events WHERE 1=1"
                delete_params: list = []
                if severity:
                    delete_query += " AND severity = ?"
                    delete_params.append(severity)
                if since:
                    since_dt = datetime.strptime(since, "%Y-%m-%d").replace(
                        tzinfo=timezone.utc
                    )
                    delete_query += " AND occurred_at >= ?"
                    delete_params.append(since_dt)
                conn.execute(delete_query, delete_params)
                click.echo(f"[ok] {len(df)} event(s) cleared from pipeline_events.")
        else:
            # Display inline
            from proto_pipe.cli.commands.view import _show_or_export
            _show_or_export(df, f"pipeline_events ({len(df)} events)", None)

    finally:
        conn.close()


# ===========================================================================
# vp status source
# ===========================================================================

@status_cmd.group(
    "source",
    cls=StageGroup,
    invoke_without_command=True,
    context_settings={"max_content_width": 120},
)
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.pass_context
def status_source(ctx, pipeline_db):
    """Show status for all sources, or drill into one.

    \b
    Examples:
      vp status source
      vp status source van
    """
    if ctx.invoked_subcommand is not None:
        return

    name = (ctx.obj or {}).get("stage_name")
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        if name:
            from proto_pipe.pipelines.query import query_source_detail
            from proto_pipe.cli.prompts import print_source_detail
            detail = query_source_detail(conn, name)
            print_source_detail(detail)
        else:
            from proto_pipe.pipelines.query import query_source_statuses
            from proto_pipe.cli.prompts import print_source_list
            statuses = query_source_statuses(conn)
            print_source_list(statuses)
    finally:
        conn.close()


# ===========================================================================
# vp status report
# ===========================================================================

@status_cmd.group(
    "report",
    cls=StageGroup,
    invoke_without_command=True,
    context_settings={"max_content_width": 120},
)
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.pass_context
def status_report(ctx, pipeline_db):
    """Show status for all reports, or drill into one.

    \b
    Examples:
      vp status report
      vp status report van_report
    """
    if ctx.invoked_subcommand is not None:
        return

    name = (ctx.obj or {}).get("stage_name")
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        if name:
            from proto_pipe.pipelines.query import query_report_detail
            from proto_pipe.cli.prompts import print_report_detail
            detail = query_report_detail(conn, name)
            print_report_detail(detail)
        else:
            from proto_pipe.pipelines.query import query_report_statuses
            from proto_pipe.cli.prompts import print_report_list
            statuses = query_report_statuses(conn)
            print_report_list(statuses)
    finally:
        conn.close()


# ===========================================================================
# vp status deliverable
# ===========================================================================

@status_cmd.group(
    "deliverable",
    cls=StageGroup,
    invoke_without_command=True,
    context_settings={"max_content_width": 120},
)
@click.pass_context
def status_deliverable(ctx):
    """Show status for all deliverables, or drill into one.

    \b
    Examples:
      vp status deliverable
      vp status deliverable monthly_pack
    """
    if ctx.invoked_subcommand is not None:
        return

    name = (ctx.obj or {}).get("stage_name")

    try:
        from proto_pipe.io.config import DeliverableConfig, load_config
        del_cfg = config_path_or_override("deliverables_config")
        config = load_config(del_cfg)
        deliverables = config.get("deliverables", [])
    except Exception:
        click.echo("  No deliverables config found.")
        return

    if not deliverables:
        click.echo("  No deliverables configured. Run: vp new deliverable")
        return

    if name:
        matched = [d for d in deliverables if d["name"] == name]
        if not matched:
            click.echo(f"[error] No deliverable named '{name}'")
            click.echo(f"Available: {', '.join(d['name'] for d in deliverables)}")
            return
        d = matched[0]
        click.echo(f"\nDeliverable: {name}\n")
        click.echo(f"  Format:  {d.get('format', 'csv')}")
        click.echo(f"  Reports: {', '.join(r['name'] for r in d.get('reports', []))}")
        if d.get("sql_file"):
            click.echo(f"  SQL:     {d['sql_file']}")
    else:
        click.echo("\nDeliverables\n")
        for d in deliverables:
            report_names = ", ".join(r["name"] for r in d.get("reports", []))
            fmt = d.get("format", "csv")
            click.echo(f"  {d['name']:<25} {fmt:<8} reports: {report_names}")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def status_commands(cli: click.Group) -> None:
    cli.add_command(status_cmd)
