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
      vp status — health overview
      vp status source — all sources, one line each
      vp status source van — one source in detail
      vp status report — all reports, one line each
      vp status --log — show pipeline events
      vp status --log --severity error — show only errors
      vp status --log --export — export events to CSV
    """
    if ctx.invoked_subcommand is not None:
        return

    p_db = config_path_or_override("pipeline_db", pipeline_db)

    if show_log:
        _handle_log(p_db, severity, since, export_log, clear)
        return

    conn = duckdb.connect(p_db)
    try:
        _print_health_summary(conn, p_db)
    finally:
        conn.close()


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


def _print_health_summary(conn: duckdb.DuckDBPyConnection, p_db: str):
    """Print one-line-per-stage health summary."""
    # --- Sources ---
    try:
        src_tables = conn.execute(
            "SELECT DISTINCT table_name FROM ingest_state WHERE status = 'ok'"
        ).df()["table_name"].tolist()
    except Exception:
        src_tables = []

    src_error_count = 0
    try:
        src_error_count = conn.execute(
            "SELECT count(*) FROM source_block"
        ).fetchone()[0]
    except Exception:
        pass

    # --- Reports ---
    try:
        report_names = conn.execute(
            "SELECT DISTINCT report_name FROM validation_pass"
        ).df()["report_name"].tolist()
    except Exception:
        report_names = []

    val_failure_count = 0
    try:
        val_failure_count = conn.execute(
            "SELECT count(*) FROM validation_block"
        ).fetchone()[0]
    except Exception:
        pass

    # --- Deliverables ---
    try:
        from proto_pipe.io.config import DeliverableConfig
        del_cfg = config_path_or_override("deliverables_config")
        del_names = DeliverableConfig(del_cfg).names()
    except Exception:
        del_names = []

    click.echo("\nPipeline Status\n")

    # Sources line
    if src_tables:
        error_note = f"    {src_error_count} error(s)" if src_error_count else ""
        click.echo(f"  Sources:       {len(src_tables)} ingested{error_note}")
    else:
        click.echo("  Sources:       none ingested yet")

    # Reports line
    if report_names:
        fail_note = f"    {val_failure_count} failure(s)" if val_failure_count else ""
        click.echo(f"  Reports:       {len(report_names)} validated{fail_note}")
    else:
        click.echo("  Reports:       none validated yet")

    # Deliverables line
    if del_names:
        click.echo(f"  Deliverables:  {len(del_names)} configured")
    else:
        click.echo("  Deliverables:  none configured")

    # Prescriptive next steps
    steps = []
    if not src_tables:
        steps.append("vp new source  →  vp ingest")
    if src_error_count:
        steps.append(f"vp errors source — {src_error_count} error(s) need resolution")
    if src_tables and not report_names:
        steps.append("vp new report  →  vp validate")
    if val_failure_count:
        steps.append(f"vp errors report — {val_failure_count} failure(s) to review")
    if report_names and not del_names:
        steps.append("vp new deliverable  →  vp deliver <name>")

    if steps:
        click.echo("\n  Next steps:")
        for s in steps:
            click.echo(f"    {s}")
    else:
        click.echo("\n  All clear — ready to deliver.")


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
            _source_detail(conn, name)
        else:
            _source_list(conn)
    finally:
        conn.close()


def _source_list(conn):
    """Print one-line summary per source table."""
    try:
        df = conn.execute("""
            SELECT
                table_name,
                count(*) AS file_count,
                sum(rows) AS total_rows,
                max(ingested_at) AS last_ingest
            FROM ingest_state
            WHERE status = 'ok'
            GROUP BY table_name
            ORDER BY table_name
        """).df()
    except Exception:
        click.echo("  No ingest history found. Run: vp ingest")
        return

    if df.empty:
        click.echo("  No sources ingested yet. Run: vp ingest")
        return

    # Get error counts per table
    try:
        errors = dict(conn.execute("""
            SELECT table_name, count(*) FROM source_block
            GROUP BY table_name
        """).fetchall())
    except Exception:
        errors = {}

    click.echo("\nSources\n")
    for _, row in df.iterrows():
        table = row["table_name"]
        rows = int(row["total_rows"]) if row["total_rows"] else 0
        last = str(row["last_ingest"])[:10] if row["last_ingest"] else "—"
        err_count = errors.get(table, 0)
        err_note = f"  {err_count} error(s)" if err_count else ""
        click.echo(f"  {table:<25} {rows:>8} rows    last: {last}{err_note}")


def _source_detail(conn, name):
    """Print detailed status for one source."""
    click.echo(f"\nSource: {name}\n")

    # Row count from actual table
    try:
        row_count = conn.execute(
            f'SELECT count(*) FROM "{name}"'
        ).fetchone()[0]
        click.echo(f"  Rows:         {row_count}")
    except Exception:
        click.echo("  Rows:         table not found")

    # Error count
    try:
        err_count = conn.execute(
            "SELECT count(*) FROM source_block WHERE table_name = ?", [name]
        ).fetchone()[0]
        if err_count:
            click.echo(f"  Errors:       {err_count}")
            click.echo(f"    Fix: vp errors source {name}")
        else:
            click.echo("  Errors:       none")
    except Exception:
        pass

    # Ingest history
    try:
        history = conn.execute("""
            SELECT filename, status, rows, ingested_at
            FROM ingest_state
            WHERE table_name = ?
            ORDER BY ingested_at DESC
            LIMIT 10
        """, [name]).df()
    except Exception:
        history = None

    if history is not None and not history.empty:
        click.echo("\n  Ingest history:")
        for _, row in history.iterrows():
            ts = str(row["ingested_at"])[:10]
            rows = row["rows"] if row["rows"] else "—"
            click.echo(f"    {ts}  {row['filename']:<35} {row['status']:<10} {rows} rows")


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
            _report_detail(conn, name)
        else:
            _report_list(conn)
    finally:
        conn.close()


def _report_list(conn):
    """Print one-line summary per report."""
    try:
        df = conn.execute("""
            SELECT
                report_name,
                count(*) AS record_count,
                max(validated_at) AS last_validated
            FROM validation_pass
            GROUP BY report_name
            ORDER BY report_name
        """).df()
    except Exception:
        click.echo("  No validation history found. Run: vp validate")
        return

    if df.empty:
        click.echo("  No reports validated yet. Run: vp validate")
        return

    # Failure counts
    try:
        failures = dict(conn.execute("""
            SELECT report_name, count(*) FROM validation_block
            GROUP BY report_name
        """).fetchall())
    except Exception:
        failures = {}

    click.echo("\nReports\n")
    for _, row in df.iterrows():
        rname = row["report_name"]
        records = int(row["record_count"])
        last = str(row["last_validated"])[:10] if row["last_validated"] else "—"
        fail_count = failures.get(rname, 0)
        fail_note = f"  {fail_count} failure(s)" if fail_count else ""
        click.echo(f"  {rname:<25} {records:>8} records    last: {last}{fail_note}")


def _report_detail(conn, name):
    """Print detailed status for one report."""
    click.echo(f"\nReport: {name}\n")

    # Row count from report table
    try:
        row_count = conn.execute(
            f'SELECT count(*) FROM "{name}"'
        ).fetchone()[0]
        click.echo(f"  Rows:         {row_count}")
    except Exception:
        click.echo("  Rows:         table not found (run vp validate)")

    # Last validated
    try:
        last = conn.execute(
            "SELECT max(validated_at) FROM validation_pass WHERE report_name = ?",
            [name],
        ).fetchone()[0]
        click.echo(f"  Last validated: {str(last)[:10] if last else '—'}")
    except Exception:
        pass

    # Failure count
    try:
        fail_count = conn.execute(
            "SELECT count(*) FROM validation_block WHERE report_name = ?", [name]
        ).fetchone()[0]
        if fail_count:
            click.echo(f"  Failures:     {fail_count}")
            click.echo(f"    Fix: vp errors report {name}")
        else:
            click.echo("  Failures:     none")
    except Exception:
        pass

    # Check breakdown
    try:
        checks = conn.execute("""
            SELECT check_name, status, count(*) AS cnt
            FROM validation_pass
            WHERE report_name = ?
            GROUP BY check_name, status
            ORDER BY check_name, status
        """, [name]).fetchall()
    except Exception:
        checks = []

    if checks:
        click.echo("\n  Check results:")
        for check_name, status, cnt in checks:
            click.echo(f"    {check_name:<30} {status:<10} {cnt} records")


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
