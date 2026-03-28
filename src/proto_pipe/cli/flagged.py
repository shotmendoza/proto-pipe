"""Flagged-row commands — flagged-summary, flagged-list, flagged-clear,
export-flagged, import-corrections, check-null-overwrites."""

from pathlib import Path

import click

from proto_pipe.cli.helpers import config_path_or_override


def _resolve_primary_key(
        table: str,
        sources_config: str | None
) -> str | None:
    """Look up the primary key for a table from sources_config.yaml.

    Returns the key string if found, or None after printing an error message.
    The caller should return early when this returns None.
    """
    from proto_pipe.io.registry import load_config

    src_cfg = config_path_or_override("sources_config", sources_config)
    try:
        _config = load_config(src_cfg)
    except FileNotFoundError:
        click.echo(f"[error] Could not find sources config at '{src_cfg}'")
        return None

    sources = {s["target_table"]: s for s in _config["sources"]}

    if table not in sources:
        click.echo(f"[error] No source defined for '{table}' in sources_config.yaml")
        click.echo("Use --key to specify the primary key directly.")
        return None

    primary_key = sources[table].get("primary_key")
    if not primary_key:
        click.echo(
            f"[error] No primary_key defined for '{table}' in sources_config.yaml"
        )
        click.echo("Add primary_key to the source definition, or use --key.")
        return None

    return primary_key


# ---------------------------------------------------------------------------
# flagged-summary
# ---------------------------------------------------------------------------


@click.command("flagged-summary")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def flagged_summary(pipeline_db):
    """Print a count of open flagged rows by table and reason.

    Use this as a quick health check after ingest or validate to see
    whether anything needs attention before running pull-report.

    \b
    Example:
      vp flagged-summary
    """
    import duckdb

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        df = conn.execute("""
            SELECT
                table_name,
                check_name,
                reason,
                count(*) AS flagged_count,
                min(flagged_at) AS first_flagged,
                max(flagged_at) AS last_flagged
            FROM flagged_rows
            GROUP BY table_name, check_name, reason
            ORDER BY table_name, flagged_count DESC
        """).df()

        if df.empty:
            click.echo("\n  No flagged rows — all clear.")
            return

        total = df["flagged_count"].sum()
        click.echo(f"\n{total} flagged row(s) across {df['table_name'].nunique()} table(s):\n")

        current_table = None
        for _, row in df.iterrows():
            if row["table_name"] != current_table:
                current_table = row["table_name"]
                click.echo(f"{current_table}")
            click.echo(f"{row['flagged_count']:>4}  [{row['check_name']}]  {row['reason']}")

        click.echo(f"\nRun: vp flagged-list --table <n>    to see affected rows")
        click.echo(f"Run: vp export-flagged --table <n>  to export for correction")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# flagged-list
# ---------------------------------------------------------------------------
@click.command("flagged-list")
@click.option("--table", required=True, help="Table to list flagged rows for.")
@click.option("--check", default=None, help="Filter by check name (e.g. duplicate_conflict).")
@click.option("--limit", default=50, show_default=True, help="Max rows to display.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def flagged_list(table, check, limit, pipeline_db):
    """Print flagged rows for a table inline in the terminal.

    Shows the flag metadata alongside the source row data so you can
    assess the issue without opening a SQL client or exporting a file.

    \b
    Examples:
      vp flagged-list --table sales
      vp flagged-list --table sales --check duplicate_conflict
      vp flagged-list --table sales --limit 20
    """
    import duckdb

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        query  = """
            SELECT id AS _flag_id, row_index, check_name, reason AS _flag_reason, flagged_at
            FROM flagged_rows
            WHERE table_name = ?
        """
        params = [table]
        if check:
            query += " AND check_name = ?"
            params.append(check)
        query += " ORDER BY flagged_at DESC"

        flags = conn.execute(query, params).df()

        if flags.empty:
            click.echo(f"\n  No flagged rows for '{table}'.")
            return

        total = len(flags)
        shown = min(total, limit)
        click.echo(f"\n  {total} flagged row(s) for '{table}' (showing {shown}):\n")

        # Pull matching source rows
        source_df = conn.execute(f'SELECT * FROM "{table}"').df()
        source_df["_row_index"] = source_df.index

        merged = (
            flags.head(limit)
            .merge(source_df, left_on="row_index", right_on="_row_index", how="left")
            .drop(columns=["_row_index", "row_index", "_flag_id"])
        )

        for _, row in merged.iterrows():
            click.echo(f"  ── [{row['check_name']}] {row['_flag_reason']}")
            click.echo(f"     flagged: {row['flagged_at']}")
            skip = {"check_name", "_flag_reason", "flagged_at"}
            for col in [c for c in merged.columns if c not in skip]:
                click.echo(f"     {col}: {row[col]}")
            click.echo()

        if total > limit:
            click.echo(
                f"  ... {total - limit} more row(s) not shown. "
                f"Use --limit or vp export-flagged to see all."
            )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# flagged-clear
# ---------------------------------------------------------------------------
@click.command("flagged-clear")
@click.option("--table", required=True, help="Table to clear flags for.")
@click.option("--check", default=None, help="Only clear flags for this check name.")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def flagged_clear(table, check, yes, pipeline_db):
    """Clear all flagged rows for a table without applying corrections.

    Use this when you've reviewed the flags and decided to proceed without
    making corrections (e.g. the data is acceptable as-is). This action
    cannot be undone.

    \b
    Examples:
      vp flagged-clear --table sales
      vp flagged-clear --table sales --check duplicate_conflict
      vp flagged-clear --table sales --yes
    """
    import duckdb

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        count_query = "SELECT count(*) FROM flagged_rows WHERE table_name = ?"
        count_params = [table]
        if check:
            count_query += " AND check_name = ?"
            count_params.append(check)

        count = conn.execute(count_query, count_params).fetchone()[0]

        if count == 0:
            click.echo(f"No flagged rows to clear for '{table}'.")
            return

        scope = f"check '{check}'" if check else "all checks"
        if not yes:
            click.confirm(
                f"Clear {count} flagged row(s) for '{table}' ({scope})? "
                f"This cannot be undone.",
                abort=True,
            )

        del_query = "DELETE FROM flagged_rows WHERE table_name = ?"
        del_params = [table]
        if check:
            del_query += " AND check_name = ?"
            del_params.append(check)

        conn.execute(del_query, del_params)
        click.echo(f"[ok] {count} flag(s) cleared for '{table}' ({scope})")
    except click.Abort:
        click.echo("\n  Cancelled.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# export-flagged
# ---------------------------------------------------------------------------
@click.command("export-flagged")
@click.option("--table", required=True, help="Table to export flagged rows from.")
@click.option("--key", default=None, help="Override primary key column (default: from sources_config.yaml).")
@click.option("--output", default=None,  help="Output CSV path. Defaults to flagged_<table>.csv in output_dir.")
@click.option("--pipeline-db", default=None,  help="Override pipeline DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
def export_flagged(table, key, output, pipeline_db, sources_config):
    """Export flagged rows for a table to CSV for manual correction.

    The file includes all source columns plus _flag_id and _flag_reason.
    Fix the values in the file, then run import-corrections to apply them.

    \b
    Example:
      vp export-flagged --table sales
      vp export-flagged --table sales --output /tmp/sales_fixes.csv
      vp export-flagged --table sales --key order_id
    """
    import duckdb
    from proto_pipe.reports.corrections import export_flagged as _export, dated_export_path

    primary_key = key or _resolve_primary_key(table, sources_config)
    if not primary_key:
        return

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    out_dir = config_path_or_override("output_dir")
    output_path = output or dated_export_path(out_dir, table)

    conn = duckdb.connect(p_db)
    try:
        count = _export(conn, table, output_path, primary_key)
        click.echo(f"[ok] {count} flagged row(s) exported to: {output_path}")
        click.echo(f"\nFix the values, then run:")
        click.echo(f"vp import-corrections {output_path} --table {table}")
    except ValueError as e:
        click.echo(f"[error] {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# import-corrections
# ---------------------------------------------------------------------------


@click.command("import-corrections")
@click.argument("filepath")
@click.option("--table", required=True, help="Table to apply corrections to.")
@click.option("--key", default=None,  help="Override primary key column (default: from sources_config.yaml).")
@click.option("--pipeline-db", default=None,  help="Override pipeline DB path.")
@click.option("--sources-config", default=None,  help="Override sources config path.")
def import_corrections(filepath, table, key, pipeline_db, sources_config):
    """Apply a corrected CSV back to the source table and clear resolved flags.

    Primary key is read from sources_config.yaml or overridden with --key.
    Only columns present in both the file and the table are updated.

    \b
    Example:
      vp import-corrections flagged_sales.csv --table sales
      vp import-corrections flagged_sales.csv --table sales --key order_id
    """
    import duckdb

    from proto_pipe.reports.corrections import import_corrections as _import
    from proto_pipe.io.registry import load_config

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    src_cfg = config_path_or_override("sources_config", sources_config)
    path = Path(filepath)

    if not path.exists():
        click.echo(f"[error] File not found: {filepath}")
        return

    # Resolve primary key: --key overrides sources_config.yaml
    primary_key = key or _resolve_primary_key(table, sources_config)
    if not primary_key:
        _config = load_config(src_cfg)
        sources = {s["target_table"]: s for s in _config["sources"]}
        if table not in sources:
            click.echo(f"[error] No source defined for '{table}' in sources_config.yaml")
            click.echo("Use --key to specify the primary key directly.")
            return
        primary_key = sources[table].get("primary_key")
        if not primary_key:
            click.echo(f"[error] No primary_key defined for '{table}' in sources_config.yaml")
            click.echo("Add primary_key to the source or use --key.")
            return

    conn = duckdb.connect(p_db)
    try:
        result = _import(conn, table, str(path), primary_key)
        click.echo(f"[ok] {result['updated']} row(s) updated in '{table}'")
        if result["flagged_cleared"]:
            click.echo(f"[ok] {result['flagged_cleared']} ingest conflict(s) cleared from flagged_rows")
        if result["validation_cleared"]:
            click.echo(f"[ok] {result['validation_cleared']} validation flag(s) cleared from validation_flags")
    except (ValueError, FileNotFoundError) as e:
        click.echo(f"[error] {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# check-null-overwrites
# ---------------------------------------------------------------------------
@click.command("check-null-overwrites")
@click.option("--table", required=True, help="Table to scan for duplicate conflicts.")
@click.option("--pipeline-db", default=None,  help="Override pipeline DB path.")
@click.option("--sources-config", default=None,  help="Override sources config path.")
def check_null_overwrites_cmd(table, pipeline_db, sources_config):
    """Manually scan a table for rows with the same primary key but different
    content, and flag any new conflicts not already in flagged_rows.

    Useful after a schema change or to audit an existing table without
    waiting for a new ingest run.

    \b
    Example:
      vp check-null-overwrites --table sales
    """
    import duckdb

    from proto_pipe.io.ingest import check_null_overwrites
    from proto_pipe.io.registry import load_config

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    src_cfg = config_path_or_override("sources_config", sources_config)

    _config = load_config(src_cfg)
    sources = {s["target_table"]: s for s in _config["sources"]}

    if table not in sources:
        click.echo(f"[error] No source defined for '{table}' in sources_config.yaml")
        return

    primary_key = _resolve_primary_key(table, sources_config)
    if not primary_key:
        click.echo(
            f"  [error] No primary_key defined for '{table}' in sources_config.yaml\n"
            f"  Add primary_key to the source definition to enable conflict detection."
        )
        return

    conn = duckdb.connect(p_db)
    try:
        flagged = check_null_overwrites(conn, table, primary_key)
        if flagged:
            click.echo(f"[ok] {flagged} new conflict(s) flagged in '{table}'")
            click.echo(f"Run: vp export-flagged --table {table}")
        else:
            click.echo(f"[ok] No new conflicts found in '{table}'")
    finally:
        conn.close()


def flagged_commands(cli):
    cli.add_command(flagged_summary)
    cli.add_command(flagged_list)
    cli.add_command(flagged_clear)
    cli.add_command(export_flagged)
    cli.add_command(import_corrections)
    cli.add_command(check_null_overwrites_cmd)
