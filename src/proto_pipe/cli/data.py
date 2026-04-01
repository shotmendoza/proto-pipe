"""Data commands — ingest from sources, update-table."""

from pathlib import Path

import click

from .helpers import config_path_or_override, load_custom_checks


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


@click.command()
@click.option("--incoming-dir",   default=None, help="Override incoming files directory.")
@click.option("--pipeline-db",    default=None, help="Override pipeline DB path.")
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

    Failures are logged to ingest_log and skipped — the run continues.
    New tables are created lazily if they weren't present at db-init.
    New columns are auto-migrated without touching existing rows.

    \b
    Example:
      vp ingest
      vp ingest --mode replace
      vp ingest --validate
    """
    from proto_pipe.io.registry import load_config, register_from_config
    from proto_pipe.io.ingest import ingest_directory
    from proto_pipe.registry.base import check_registry, report_registry

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
    summary = ingest_directory(
        inc_dir,
        _config["sources"],
        p_db,
        mode=mode,
        run_checks=validate,
        check_registry=cr,
        report_registry=rr,
    )

    ok = sum(1 for v in summary.values() if v["status"] == "ok")
    skipped = sum(1 for v in summary.values() if v["status"] == "skipped")
    failed = sum(1 for v in summary.values() if v["status"] == "failed")
    flagged = sum(v.get("flagged", 0) for v in summary.values())

    click.echo(
        f"\n  {ok} loaded, {skipped} skipped, {failed} failed, "
        f"{flagged} row conflict(s) flagged — see ingest_log for details."
    )

    if failed:
        click.echo("Run: vp ingest-log --status failed  to see failure reasons.")


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

    Use this after `vp ingest` reports failures to find out why a file
    didn't load. Filter by --status failed to focus on problems only.

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
                 FROM ingest_log
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
        click.echo(f"[error] Could not read ingest_log: {e}")
        click.echo("Has `vp db-init` been run yet?")
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
    """Updates the data table in the pipeline database based on the provided file and configuration.
    This function processes updates to a specific database table by ingesting data from a file.
    It supports two modes: appending to the existing table or replacing the table's contents
    entirely. The function also handles schema migration for the database table if necessary.

    Use this to manually load a single file without scanning the whole
    incoming directory. Respects the same append/replace modes as ingest.

    :param table: Name of the target database table to update
    :param filepath: Path to the file containing data to ingest
    :param pipeline_db: (Optional) Custom path to the DuckDB pipeline database.
        If not provided, the default location is used
    :param sources_config: (Optional) Path to the YAML file defining source configurations.
        If not provided, the default configuration path is used
    :param mode: Operation mode for updating the table. Choices are "append" to add new rows
        and "replace" to recreate the table. The default mode is "append"

    \b
    Example:
        - `vp update-table sales data/incoming/sales_2026-03.csv`
        - `vp update-table sales data/incoming/sales_2026-03.csv --mode replace`
    """
    import duckdb

    from proto_pipe.io.ingest import (
        load_file,
    )
    from ..io.migration import _auto_migrate
    from ..io.db import table_exists
    from ..io.db import log_ingest
    from ..io.db import init_ingest_log
    from proto_pipe.io.registry import load_config

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
        return

    try:
        df = load_file(path)
    except Exception as e:
        click.echo(f"[error] Could not load '{filepath}': {e}")
        return

    conn = duckdb.connect(p_db)
    init_ingest_log(conn)

    if mode == "replace" or not table_exists(conn, table):
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM df')
        new_cols = []
        click.echo(f"[ok] '{table}' replaced from '{path.name}' ({len(df)} rows)")
    else:
        new_cols = _auto_migrate(conn, table, df)
        conn.execute(f'INSERT INTO "{table}" SELECT * FROM df')
        click.echo(f"[ok] '{table}' updated from '{path.name}' ({len(df)} rows appended)")

    log_ingest(conn, path.name, table, "ok", rows=len(df), new_cols=new_cols)
    conn.close()


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
def data_commands(cli):
    cli.add_command(ingest)
    cli.add_command(ingest_log)
    cli.add_command(update_table)
