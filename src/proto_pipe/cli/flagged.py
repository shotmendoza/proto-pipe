"""Flagged and validated commands.

vp flagged — view raw flagged_rows (ingest conflicts)
vp flagged edit — enriched editable view joined to source table
vp flagged clear — clear flags without applying corrections
vp flagged retry — apply corrected file, clear resolved flags
vp validated — view validation_flags (check failures)

Kept as top-level:
  vp check-null-overwrites
"""

from pathlib import Path

import click
import duckdb
import pandas as pd

from proto_pipe.io.config import config_path_or_override


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _glob_flagged_export(table: str, output_dir: str):
    """Return the most recently *modified* flagged export CSV for a table.

    Globs output_dir for flagged_{table}_*.csv and returns the file with
    the highest st_mtime. Returns None if no match found.

    Modified time is used (not filename date) so the file the user most
    recently saved is always picked up by retry, regardless of when the
    export was originally created.

    Known limitation: opening and saving without changes also advances
    mtime. This is accepted — retrying with an unchanged file is harmless.
    """
    out = Path(output_dir)
    candidates = sorted(
        out.glob(f"flagged_{table}_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _resolve_primary_key(table: str, sources_config: str | None) -> str | None:
    """Look up the primary key for a table from sources_config.yaml."""
    from proto_pipe.io.config import SourceConfig

    src_cfg = config_path_or_override("sources_config", sources_config)
    try:
        config = SourceConfig(src_cfg)
    except FileNotFoundError:
        click.echo(f"[error] Could not find sources config at '{src_cfg}'")
        return None

    source = config.get_by_table(table)
    if source is None:
        click.echo(f"[error] No source defined for '{table}' in sources_config.yaml")
        click.echo("Use --key to specify the primary key directly.")
        return None

    primary_key = source.get("primary_key")
    if not primary_key:
        click.echo(
            f"[error] No primary_key defined for '{table}' in sources_config.yaml"
        )
        click.echo("Add primary_key to the source definition, or use --key.")
        return None

    return primary_key


def _get_flagged_df(
    conn: duckdb.DuckDBPyConnection,
    table: str | None,
) -> "pd.DataFrame":
    """Return raw flagged_rows, optionally filtered by table_name."""
    query = """
        SELECT
            table_name,
            check_name,
            reason,
            flagged_at,
            id AS _flag_id
        FROM flagged_rows
    """
    params = []
    if table:
        query += " WHERE table_name = ?"
        params.append(table)
    query += " ORDER BY flagged_at DESC"
    return conn.execute(query, params).df()


def _get_enriched_flagged_df(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    pk_col: str,
) -> pd.DataFrame:
    """Join flagged_rows to the source table via md5 identity.

    flagged_rows.id = md5(str(pk_value)) — set at flag time in ingest.
    Join: md5(CAST(source.pk_col AS VARCHAR)) = flagged_rows.id
    """
    source_cols = conn.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ?
        AND column_name NOT LIKE '\\_%' ESCAPE '\\'
        ORDER BY ordinal_position
    """, [table]).df()["column_name"].tolist()

    if not source_cols:
        return _get_flagged_df(conn, table)

    col_select = ", ".join([f's."{c}"' for c in source_cols])

    return conn.execute(f"""
        SELECT
            f.id          AS _flag_id,
            f.check_name  AS _check_name,
            f.reason      AS _flag_reason,
            f.flagged_at  AS _flagged_at,
            {col_select}
        FROM flagged_rows f
        LEFT JOIN "{table}" s
            ON md5(CAST(s."{pk_col}" AS VARCHAR)) = f.id
        WHERE f.table_name = ?
        ORDER BY f.flagged_at DESC
    """, [table]).df()


# ---------------------------------------------------------------------------
# vp flagged (group)
# ---------------------------------------------------------------------------

@click.group(
    "flagged",
    invoke_without_command=True,
    context_settings={"max_content_width": 120},
)
@click.option("--table", default=None, help="Filter by source table name.")
@click.option(
    "--export",
    default=None,
    type=click.Choice(["csv", "term"]),
    help="Export format: csv writes a file, term prints to terminal.",
)
@click.option("--limit", default=500, show_default=True, help="Max rows to display.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.pass_context
def flagged_cmd(ctx, table, export, limit, pipeline_db):
    """View ingest-time flagged rows (duplicate conflicts, type errors).

    Shows the raw flagged_rows table through a rich paged display.
    Use --table to scope to one source table.

    \b
    Examples:
      vp flagged
      vp flagged --table sales
      vp flagged --export csv
      vp flagged edit --table sales
      vp flagged clear --table sales
      vp flagged retry corrected.csv --table sales
    """
    if ctx.invoked_subcommand is not None:
        return

    from proto_pipe.cli.commands.table import get_reviewer

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)

    try:
        df = _get_flagged_df(conn, table)

        if df.empty:
            scope = f"'{table}'" if table else "any table"
            click.echo(f"\n  No flagged rows for {scope} — all clear.")
            return

        df = df.head(limit)
        title = f"flagged_rows{f' — {table}' if table else ''} ({len(df)} rows)"

        if export == "csv":
            from proto_pipe.reports.corrections import dated_export_path
            out_dir = config_path_or_override("output_dir")
            table_label = table or "all"
            output_path = dated_export_path(out_dir, table_label)
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
            click.echo(f"[ok] {len(df)} row(s) exported to: {output_path}")
        else:
            reviewer = get_reviewer(edit=False)
            reviewer.show(df, title=title)

    finally:
        conn.close()


@flagged_cmd.command("edit")
@click.option("--table", required=True, help="Source table to review and edit.")
@click.option("--key", default=None, help="Override primary key column.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def flagged_edit(table, key, sources_config, pipeline_db):
    """Open flagged rows in an enriched editable view joined to source data.

    Requires textual (uv add 'proto-pipe[tui]') for inline editing.
    Falls back to a read-only rich display if textual is not installed.
    Edits are saved via 'vp flagged retry' logic automatically on save.

    \b
    Example:
      vp flagged edit --table sales
    """
    from proto_pipe.cli.commands.table import get_reviewer
    from proto_pipe.reports.corrections import import_corrections
    import tempfile
    import os

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    pk_col = key or _resolve_primary_key(table, sources_config)

    if not pk_col:
        click.echo(
            f"[error] Cannot build enriched view without a primary key for '{table}'.\n"
            f"Set primary_key in sources_config.yaml or use --key."
        )
        return

    conn = duckdb.connect(p_db)
    try:
        df = _get_enriched_flagged_df(conn, table, pk_col)

        if df.empty:
            click.echo(f"\n  No flagged rows for '{table}'.")
            return

        title = f"flagged — {table} ({len(df)} rows, enriched)"
        reviewer = get_reviewer(edit=True)
        edited_df = reviewer.edit(df, title=title, pk_col=pk_col)

        if edited_df is not None and not edited_df.equals(df):
            save_cols = [
                c for c in edited_df.columns
                if c not in ("_check_name", "_flag_reason", "_flagged_at")
            ]
            corrections_df = edited_df[save_cols]

            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
                corrections_path = f.name
            try:
                corrections_df.to_csv(corrections_path, index=False)
                result = import_corrections(conn, table, corrections_path, pk_col)
                click.echo(f"[ok] {result['updated']} row(s) updated in '{table}'")
                if result["flagged_cleared"]:
                    click.echo(
                        f"[ok] {result['flagged_cleared']} flag(s) cleared from flagged_rows"
                    )
            finally:
                os.unlink(corrections_path)
        else:
            click.echo("  No changes made.")

    except Exception as e:
        click.echo(f"[error] Could not build enriched view: {e}")
        click.echo("  Run 'vp flagged --table' for the raw flags view.")
    finally:
        conn.close()


@flagged_cmd.command("clear")
@click.option("--table", required=True, help="Table to clear flags for.")
@click.option("--check", default=None, help="Only clear flags for this check name.")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def flagged_clear(table, check, yes, pipeline_db):
    """Clear flagged rows for a table without applying corrections.

    Use this when you've reviewed the flags and decided to proceed without
    making corrections. This action cannot be undone.

    \b
    Examples:
      vp flagged clear --table sales
      vp flagged clear --table sales --check duplicate_conflict --yes
    """
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


@flagged_cmd.command("retry")
@click.argument("table")
@click.option("--key", default=None, help="Override primary key column.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
def flagged_retry(table, key, pipeline_db, sources_config):
    """Apply the most recent flagged export for a table through the ingest cycle.

    Globs output_dir for the most recently modified flagged_{table}_*.csv,
    strips flag metadata columns, and runs the file through ingest with
    on_duplicate=upsert. Corrections are logged as status='correction' in
    ingest_log for auditability. Resolved flags are cleared from flagged_rows.

    Typical workflow:
      vp flagged open sales      # export enriched view + open for editing
      ... edit and save the CSV ...
      vp flagged retry sales     # apply corrections through ingest

    \b
    Examples:
      vp flagged retry sales
      vp flagged retry policies --key policy_id
    """
    from proto_pipe.io.config import config_path_or_override, SourceConfig
    from proto_pipe.io.ingest import ingest_single_file

    out_dir = config_path_or_override("output_dir")
    path = _glob_flagged_export(table, out_dir)

    if not path:
        click.echo(
            f"[error] No flagged export found for '{table}' in {out_dir}.\n"
            f"  Run: vp flagged open {table}"
        )
        return

    # Resolve source definition — needed for ingest cycle
    src_cfg = config_path_or_override("sources_config", sources_config)
    config = SourceConfig(src_cfg)
    source = config.get_by_table(table)
    if not source:
        click.echo(
            f"[error] No source found for table '{table}' in sources_config.yaml"
        )
        return

    primary_key = key or source.get("primary_key")
    if not primary_key:
        click.echo(
            f"[error] No primary key for '{table}' — cannot apply targeted corrections.\n"
            f"  Add primary_key to sources_config.yaml or use --key."
        )
        return

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    click.echo(f"  Applying corrections from: {path.name}")

    conn = duckdb.connect(p_db)
    try:
        result = ingest_single_file(
            conn=conn,
            path=path,
            source=source,
            on_duplicate_override="upsert",
            log_status_override="correction",
            strip_pipeline_cols=True,
        )

        if result["status"] != "ok":
            click.echo(f"[error] {result.get('message', 'Ingest failed')}")
            return

        click.echo(f"[ok] {result['rows']} row(s) applied to '{table}'")

        # Clear flags for this table — corrections resolved them
        cleared = conn.execute(
            "DELETE FROM flagged_rows WHERE table_name = ? RETURNING id",
            [table],
        ).fetchall()
        if cleared:
            click.echo(f"[ok] {len(cleared)} flag(s) cleared from flagged_rows")
        else:
            click.echo("  No flags to clear.")

        if result.get("new_cols"):
            click.echo(f"  New columns added: {', '.join(result['new_cols'])}")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp validated
# ---------------------------------------------------------------------------

@click.command("validated")
@click.option("--table", default=None, help="Filter by source table name.")
@click.option("--report", default=None, help="Filter by report name.")
@click.option(
    "--export",
    default=None,
    type=click.Choice(["csv", "term"]),
    help="Export format: csv writes a file, term prints to terminal.",
)
@click.option("--limit", default=500, show_default=True, help="Max rows to display.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def validated_cmd(table, report, export, limit, pipeline_db):
    """View validation flags (check failures from vp validate).

    These warn but do not block deliverables. The correction path is:
    fix at source → re-ingest → re-validate.

    \b
    Examples:
      vp validated
      vp validated --report daily_sales_validation
      vp validated --table sales
      vp validated --export csv
    """
    from proto_pipe.cli.commands.table import get_reviewer
    from proto_pipe.reports.validation_flags import detail_df

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)

    try:
        try:
            df = detail_df(conn, report_name=report)
        except Exception as e:
            click.echo(f"[error] Could not read validation_flags: {e}")
            click.echo("Has 'vp db-init' been run yet?")
            return

        if table:
            df = df[df["table_name"] == table].copy()

        if df.empty:
            parts = []
            if report:
                parts.append(f"report '{report}'")
            if table:
                parts.append(f"table '{table}'")
            scope = " / ".join(parts) if parts else "any report"
            click.echo(f"\n  No validation flags for {scope} — all clear.")
            return

        df = df.head(limit)
        display_df = df.drop(columns=["_flag_id"], errors="ignore")

        parts = []
        if report:
            parts.append(report)
        if table:
            parts.append(table)
        label = " / ".join(parts) if parts else "all reports"
        title = f"validation_flags — {label} ({len(display_df)} rows)"

        if export == "csv":
            from proto_pipe.reports.corrections import dated_export_path
            out_dir = config_path_or_override("output_dir")
            label_safe = (report or table or "all").replace(" ", "_")
            output_path = dated_export_path(out_dir, f"validated_{label_safe}")
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            display_df.to_csv(output_path, index=False)
            click.echo(f"[ok] {len(display_df)} row(s) exported to: {output_path}")
        else:
            reviewer = get_reviewer(edit=False)
            reviewer.show(display_df, title=title)

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Kept as top-level: check-null-overwrites
# ---------------------------------------------------------------------------

@click.command("check-null-overwrites")
@click.option("--table", required=True, help="Table to scan for duplicate conflicts.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
def check_null_overwrites_cmd(table, pipeline_db, sources_config):
    """Scan a table for rows with the same primary key but different content.

    \b
    Example:
      vp check-null-overwrites --table sales
    """
    from proto_pipe.io.ingest import check_null_overwrites

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    primary_key = _resolve_primary_key(table, sources_config)
    if not primary_key:
        return

    conn = duckdb.connect(p_db)
    try:
        flagged = check_null_overwrites(conn, table, primary_key)
        if flagged:
            click.echo(f"[ok] {flagged} new conflict(s) flagged in '{table}'")
            click.echo(f"Run: vp flagged --table {table}")
        else:
            click.echo(f"[ok] No new conflicts found in '{table}'")
    finally:
        conn.close()


@flagged_cmd.command("open")
@click.argument("table")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def flagged_open(table, sources_config, pipeline_db):
    """Export flagged rows for a table and open the file for editing.

    If a flagged export already exists in output_dir, opens the most
    recently modified one. Otherwise auto-exports first, then opens.

    Exports the enriched view (source data alongside flag context) so you
    can see and fix the actual bad values directly in the file. After
    editing and saving, run:

        vp flagged retry <table>

    to apply the corrections back through the full ingest cycle.

    \b
    Examples:
      vp flagged open sales
      vp flagged open policies --pipeline-db /mnt/shared/pipeline.db
    """
    from proto_pipe.io.config import config_path_or_override
    from proto_pipe.reports.corrections import dated_export_path

    out_dir = config_path_or_override("output_dir")
    path = _glob_flagged_export(table, out_dir)

    if not path:
        # No existing export — auto-export enriched view so user sees source data
        p_db = config_path_or_override("pipeline_db", pipeline_db)
        pk_col = _resolve_primary_key(table, sources_config)

        conn = duckdb.connect(p_db)
        try:
            if pk_col:
                df = _get_enriched_flagged_df(conn, table, pk_col)
            else:
                df = _get_flagged_df(conn, table)

            if df.empty:
                click.echo(f"  No flagged rows for '{table}' — nothing to open.")
                return

            output_path = dated_export_path(out_dir, table)
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
            click.echo(f"[ok] {len(df)} row(s) exported to: {output_path}")
            path = Path(output_path)
        finally:
            conn.close()

    click.echo(f"  Opening: {path}")
    click.launch(str(path))
    click.echo(f"\n  Edit the file, save it, then run:")
    click.echo(f"vp flagged retry {table}")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def flagged_commands(cli):
    cli.add_command(flagged_cmd)
    cli.add_command(validated_cmd)
    cli.add_command(check_null_overwrites_cmd)
