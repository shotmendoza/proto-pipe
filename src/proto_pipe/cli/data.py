"""Data commands — ingest from sources, update-table."""

from pathlib import Path

import click

from ..checks.helpers import load_custom_checks
from ..io.config import config_path_or_override, load_config


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


@click.command()
@click.option("--incoming-dir", default=None, help="Override incoming files directory.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option(
    "--mode",
    default="append",
    show_default=True,
    type=click.Choice(["append", "replace"]),
    help="append: add rows. replace: rebuild table from file.",
)
@click.option(
    "--validate",
    is_flag=True,
    default=False,
    help="Run registered checks immediately after each file loads.",
)
def ingest(incoming_dir, pipeline_db, sources_config, mode, validate):
    """Scan the incoming directory and load matching files into DuckDB.

    Failures are logged to ingest_state and skipped — the run continues.
    New tables are created lazily if they weren't present at db-init.
    New columns are auto-migrated without touching existing rows.

    \b
    Example:
      vp ingest
      vp ingest --mode replace
      vp ingest --validate
    """
    from proto_pipe.io.registry import register_from_config
    from proto_pipe.io.ingest import ingest_directory
    from proto_pipe.checks.registry import check_registry, report_registry
    from proto_pipe.io.db import write_pipeline_events
    from proto_pipe.cli.prompts import IngestProgressReporter

    inc_dir = config_path_or_override("incoming_dir", incoming_dir)
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    src_cfg = config_path_or_override("sources_config", sources_config)
    _config = load_config(src_cfg)

    cr, rr = (check_registry, report_registry) if validate else (None, None)
    if validate:
        load_custom_checks(check_registry)
        rep_cfg = load_config(config_path_or_override("reports_config"))
        register_from_config(rep_cfg, cr, rr)

    click.echo(f"\nIngesting from: {inc_dir}")
    with IngestProgressReporter() as reporter:
        summary = ingest_directory(
            inc_dir,
            _config["sources"],
            p_db,
            mode=mode,
            run_checks=validate,
            check_registry=cr,
            report_registry=rr,
            on_file_start=reporter.on_file_start,
            on_file_done=reporter.on_file_done,
        )

    ok = sum(1 for v in summary.values() if v["status"] == "ok")
    skipped = sum(1 for v in summary.values() if v["status"] == "skipped")
    failed = sum(1 for v in summary.values() if v["status"] == "failed")
    flagged = sum(v.get("flagged", 0) for v in summary.values())

    parts = [f"{ok} loaded"]
    if failed:
        parts.append(f"{failed} failed")
    if flagged:
        parts.append(f"{flagged} row conflict(s) flagged")
    click.echo(f"\n  {', '.join(parts)}.")

    if skipped:
        click.echo(f"  {skipped} file(s) skipped (already ingested)")

    if failed:
        click.echo(
            "  Run: vp errors source  to see failure details."
        )

    # Write pipeline events — one per file processed (skipped files omitted)
    events = [
        {
            "event_type": "ingest_ok" if v["status"] == "ok" else "ingest_failed",
            "source_name": filename,
            "severity": "info" if v["status"] == "ok" else "error",
            "detail": v.get("message", ""),
        }
        for filename, v in summary.items()
        if v["status"] != "skipped"
    ]
    write_pipeline_events(p_db, events)


# ---------------------------------------------------------------------------
# ingest-log
# ---------------------------------------------------------------------------


@click.command("ingest-log")
@click.option(
    "--status",
    default=None,
    type=click.Choice(["ok", "failed", "skipped"]),
    help="Filter by status.",
)
@click.option("--table", default=None, help="Filter by target table name.")
@click.option("--limit", default=50, show_default=True, help="Max rows to display.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def ingest_log(status, table, limit, pipeline_db):
    """Show recent ingest attempts — including failures and their reasons.

    Queries the ingest_state table (was: ingest_log). Use after `vp ingest`
    to find out why a file didn't load. Filter by --status failed for problems.

    \b
    Examples:
      vp ingest-log
      vp ingest-log --status failed
      vp ingest-log --table sales
      vp ingest-log --status failed --limit 10
    """
    import duckdb

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        query = """
                 SELECT filename, table_name, status, rows, message, ingested_at
                 FROM ingest_state
                 WHERE 1=1 \
                 """
        params = []
        if status:
            query += " AND status = ?"
            params.append(status)
        if table:
            query += " AND table_name = ?"
            params.append(table)
        query += " ORDER BY ingested_at DESC"
        if limit:
            query += f" LIMIT {limit}"

        df = conn.execute(query, params).df()

        if df.empty:
            click.echo("\n  No ingest records found.")
            return

        click.echo(f"\n  {len(df)} record(s):\n")
        for _, row in df.iterrows():
            mark = (
                "✓"
                if row["status"] == "ok"
                else ("–" if row["status"] == "skipped" else "✗")
            )
            table_str = f" → {row['table_name']}" if row["table_name"] else ""
            rows_str = (
                f" ({int(row['rows'])} rows)" if row["rows"] and row["rows"] > 0 else ""
            )
            click.echo(f"{mark} {row['filename']}{table_str}{rows_str}")
            click.echo(f"{row['status']}  {row['ingested_at']}")
            if row["message"]:
                click.echo(f"reason: {row['message']}")
            click.echo()

    except Exception as e:
        click.echo(f"[error] Could not read ingest_state: {e}")
        click.echo("Has `vp init db` been run yet?")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# update-table
# ---------------------------------------------------------------------------
@click.command("update-table")
@click.argument("table")
@click.argument("filepath")
@click.option("--pipeline-db",    default=None, help="Override pipeline DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option(
    "--mode",
    default="append",
    show_default=True,
    type=click.Choice(["append", "replace"]),
    help="append: add rows. replace: rebuild table from this file.",
)
def update_table(table, filepath, pipeline_db, sources_config, mode):
    """Load a single file into a table without scanning the incoming directory.

    Routes through the full ingest path — type validation, source_pass
    tracking, and ingest_state logging all apply. Equivalent to dropping
    the file in incoming_dir and running vp ingest, but targeted.

    \b
    Examples:
      vp update-table van data/incoming/van_2026-03.csv
      vp update-table van data/incoming/van_2026-03.csv --mode replace
    """
    import duckdb

    from proto_pipe.io.ingest import ingest_single_file
    from ..io.config import load_config

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    src_cfg = config_path_or_override("sources_config", sources_config)
    path = Path(filepath)

    if not path.exists():
        click.echo(f"  [error] File not found: {filepath}")
        return

    config = load_config(src_cfg)
    sources = {s["target_table"]: s for s in config["sources"]}

    if table not in sources:
        click.echo(f"[error] No source defined for table '{table}' in sources_config.yaml")
        click.echo("  Run: vp new source — to configure this table as a source.")
        return

    source = sources[table]
    conn = duckdb.connect(p_db)
    try:
        result = ingest_single_file(conn, path, source, mode=mode)
    finally:
        conn.close()

    if result["status"] == "ok":
        action = "replaced" if mode == "replace" else "updated"
        click.echo(
            f"[ok] '{table}' {action} from '{path.name}' "
            f"({result['rows']} inserted, {result['flagged']} blocked, "
            f"{result['skipped']} skipped)"
        )
        if result.get("new_cols"):
            click.echo(f"  New columns added: {', '.join(result['new_cols'])}")
        if result["flagged"]:
            click.echo(f"  Run: vp errors source {table}")
    else:
        click.echo(f"[error] {result.get('message', 'Ingest failed')}")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
def data_commands(cli):
    cli.add_command(ingest)
    cli.add_command(ingest_log)
    cli.add_command(update_table)
