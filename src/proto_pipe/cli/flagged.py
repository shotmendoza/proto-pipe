"""Flagged and validated commands.

vp flagged — view raw source_block (ingest conflicts)
vp flagged edit — enriched editable view built from source files via flagging.py
vp flagged open — export enriched view to incoming_dir, open for editing
vp flagged clear — clear flags without applying corrections
vp flagged retry — re-ingest corrected flagged export, bypass hash for accepted rows

vp validated — view raw validation_block (check/transform failures)
vp validated edit — enriched editable view joined to report table
vp validated open — export to incoming_dir for external editing
vp validated clear — clear validation blocks without corrections
vp validated retry — upsert corrected records back to report table

Kept as top-level:
  vp check-null-overwrites
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import click
import duckdb
import pandas as pd

from proto_pipe.io.config import config_path_or_override
from proto_pipe.io.db import coerce_for_display


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _resolve_source(table: str, sources_config: str | None) -> dict | None:
    """Return the source config dict for a table, or None."""
    from proto_pipe.io.config import SourceConfig
    src_cfg = config_path_or_override("sources_config", sources_config)
    try:
        return SourceConfig(src_cfg).get_by_table(table)
    except FileNotFoundError:
        click.echo(f"[error] Could not find sources config at '{src_cfg}'")
        return None


def _resolve_primary_key(table: str, sources_config: str | None) -> str | None:
    """Look up the primary key for a table from sources_config.yaml."""
    source = _resolve_source(table, sources_config)
    if source is None:
        click.echo(f"[error] No source defined for '{table}' in sources_config.yaml")
        click.echo("Use --key to specify the primary key directly.")
        return None
    pk = source.get("primary_key")
    if not pk:
        click.echo(f"[error] No primary_key defined for '{table}' in sources_config.yaml")
        click.echo("Add primary_key to the source definition, or use --key.")
    return pk


def _glob_flagged_export(table: str, incoming_dir: str) -> Path | None:
    """Return the most recently modified flagged export CSV for a table.

    Globs incoming_dir for *_flagged_*.csv files and returns the most
    recently modified one. Named to match source patterns so vp ingest
    picks them up naturally (e.g. van_flagged_2026-04-02.csv matches van_*.csv).
    """
    inc = Path(incoming_dir)
    candidates = sorted(
        list(inc.glob(f"*{table}*_flagged_*.csv")) +
        list(inc.glob(f"{table}_flagged_*.csv")),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _flagged_export_path(table: str, source_name: str, incoming_dir: str) -> str:
    """Build the export path for a flagged table export.

    Named <source_name>_flagged_<date>.csv so it matches the source pattern
    and is picked up by vp ingest / vp flagged retry automatically.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return str(Path(incoming_dir) / f"{source_name}_flagged_{today}.csv")


def _get_raw_source_block(
    conn: duckdb.DuckDBPyConnection,
    table: str | None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Return raw source_block, optionally filtered by table_name."""
    query = """
        SELECT
            id          AS _flag_id,
            table_name,
            check_name  AS _flag_check,
            pk_value,
            source_file,
            source_file_missing,
            bad_columns AS _flag_columns,
            reason      AS _flag_reason,
            flagged_at
        FROM source_block
    """
    params = []
    if table:
        query += " WHERE table_name = ?"
        params.append(table)
    query += " ORDER BY flagged_at DESC"
    if limit:
        query += f" LIMIT {limit}"
    return coerce_for_display(conn.execute(query, params).df())


def _get_raw_validation_block(
    conn: duckdb.DuckDBPyConnection,
    table: str | None,
    report: str | None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Return raw validation_block, optionally filtered."""
    query = """
        SELECT
            id          AS _flag_id,
            table_name,
            report_name,
            check_name  AS _flag_check,
            pk_value,
            bad_columns AS _flag_columns,
            reason      AS _flag_reason,
            flagged_at
        FROM validation_block
        WHERE 1=1
    """
    params = []
    if table:
        query += " AND table_name = ?"
        params.append(table)
    if report:
        query += " AND report_name = ?"
        params.append(report)
    query += " ORDER BY flagged_at DESC"
    if limit:
        query += f" LIMIT {limit}"
    return coerce_for_display(conn.execute(query, params).df())


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
    """View ingest-time blocked rows (duplicate conflicts, type errors).

    Shows the raw source_block table. Use --table to scope to one source.

    \b
    Examples:
      vp flagged
      vp flagged --table van
      vp flagged --export csv
      vp flagged edit --table van
      vp flagged open van
      vp flagged clear --table van
      vp flagged retry van
    """
    if ctx.invoked_subcommand is not None:
        return

    from proto_pipe.cli.commands.table import get_reviewer

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)

    try:
        df = _get_raw_source_block(conn, table, limit=limit)

        if df.empty:
            scope = f"'{table}'" if table else "any table"
            click.echo(f"\n  No blocked rows for {scope} — all clear.")
            return

        title = f"source_block{f' — {table}' if table else ''} ({len(df)} rows)"

        if export == "csv":
            from proto_pipe.reports.corrections import dated_export_path
            out_dir = config_path_or_override("output_dir")
            table_label = table or "all"
            output_path = dated_export_path(out_dir, f"source_block_{table_label}")
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
@click.option(
    "--resolve",
    is_flag=True,
    default=False,
    help="Wizard mode — step through each conflict for column-by-column resolution.",
)
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def flagged_edit(table, key, resolve, sources_config, pipeline_db):
    """Open blocked rows in an enriched editable TUI view.

    Globs source files and joins to source_block so you see the actual
    bad values alongside the flag reason. Edit inline and save — changes
    are applied through the full ingest path with upsert.

    Use --resolve for a wizard that steps through each conflict.

    \b
    Examples:
      vp flagged edit --table van
      vp flagged edit --table van --resolve
    """
    from proto_pipe.cli.commands.table import get_reviewer
    from proto_pipe.pipelines.flagging import build_source_flag_export
    import tempfile, os

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    pk_col = key or _resolve_primary_key(table, sources_config)
    if not pk_col:
        click.echo(
            f"[error] Cannot build enriched view without a primary key for '{table}'.\n"
            f"  Set primary_key in sources_config.yaml or use --key."
        )
        return

    conn = duckdb.connect(p_db)
    try:
        df = build_source_flag_export(conn, table, pk_col)
        df = coerce_for_display(df)

        if df.empty:
            click.echo(f"\n  No blocked rows for '{table}' — all clear.")
            return

        title = f"source_block — {table} ({len(df)} rows, enriched)"

        if resolve:
            _run_resolve_wizard(conn, df, table, pk_col)
            return

        reviewer = get_reviewer(edit=True)
        edited_df = reviewer.edit(df, title=title, pk_col=pk_col)

        if edited_df is None or edited_df.equals(df):
            click.echo("  No changes made.")
            return

        _apply_flagged_edits(conn, table, pk_col, df, edited_df, sources_config)

    except Exception as e:
        click.echo(f"[error] Could not build enriched view: {e}")
        click.echo("  Run 'vp flagged --table' for the raw flags view.")
    finally:
        conn.close()


def _run_resolve_wizard(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table: str,
    pk_col: str,
) -> None:
    """Step-through wizard for resolving duplicate conflicts column by column."""
    import questionary

    flag_col = "_flag_columns"
    reason_col = "_flag_reason"

    for _, row in df.iterrows():
        pk_val = row.get(pk_col, "unknown")
        reason = row.get(reason_col, "")
        bad_cols = [c.strip() for c in str(row.get(flag_col, "")).split("|") if c.strip()]

        click.echo(f"\n── Record: {pk_col}={pk_val} ──────────────────────────")
        click.echo(f"  Reason: {reason}")

        action = questionary.select(
            "What would you like to do with this record?",
            choices=[
                questionary.Choice("Keep existing (discard incoming change)", value="skip"),
                questionary.Choice("Accept incoming (overwrite existing)", value="accept"),
                questionary.Choice("Skip for now", value="later"),
            ],
        ).ask()

        if action == "skip":
            conn.execute(
                "DELETE FROM source_block WHERE id = ?",
                [row.get("_flag_id")]
            )
            click.echo(f"  [ok] Flag cleared — existing row kept.")
        elif action == "accept":
            # Mark for upsert by clearing flag — user will re-ingest
            conn.execute(
                "DELETE FROM source_block WHERE id = ?",
                [row.get("_flag_id")]
            )
            click.echo(f"  [ok] Flag cleared — re-ingest the source file to apply.")
        else:
            click.echo(f"  [skip] Left for later.")


def _apply_flagged_edits(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    pk_col: str,
    original_df: pd.DataFrame,
    edited_df: pd.DataFrame,
    sources_config: str | None,
) -> None:
    """Apply edits from the TUI back to the source table and clear resolved flags."""
    from proto_pipe.io.ingest import ingest_single_file
    from proto_pipe.io.config import SourceConfig
    import tempfile, os

    # Determine which rows were deleted (user wants to skip)
    orig_flag_ids = set(original_df["_flag_id"].dropna().tolist())
    edited_flag_ids = set(edited_df["_flag_id"].dropna().tolist()) if "_flag_id" in edited_df.columns else set()
    deleted_flag_ids = orig_flag_ids - edited_flag_ids

    # Clear deleted flags (user chose to skip)
    if deleted_flag_ids:
        placeholders = ", ".join(["?"] * len(deleted_flag_ids))
        conn.execute(
            f"DELETE FROM source_block WHERE id IN ({placeholders})",
            list(deleted_flag_ids),
        )
        click.echo(f"  [ok] {len(deleted_flag_ids)} row(s) skipped and cleared from source_block")

    # Apply edits for remaining rows
    data_cols = [c for c in edited_df.columns if not c.startswith("_")]
    corrections_df = edited_df[data_cols].copy()

    if corrections_df.empty:
        return

    src_cfg = config_path_or_override("sources_config", sources_config)
    try:
        source = SourceConfig(src_cfg).get_by_table(table)
    except Exception:
        source = {"target_table": table, "primary_key": pk_col, "patterns": []}

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        corrections_path = f.name
    try:
        corrections_df.to_csv(corrections_path, index=False)
        result = ingest_single_file(
            conn=conn,
            path=Path(corrections_path),
            source=source or {"target_table": table, "primary_key": pk_col, "patterns": []},
            on_duplicate_override="upsert",
            log_status_override="correction",
            strip_pipeline_cols=True,
        )
        if result["status"] == "ok":
            click.echo(f"[ok] {result['rows']} row(s) applied to '{table}'")
            # Clear resolved flags
            remaining_flag_ids = list(edited_flag_ids)
            if remaining_flag_ids:
                placeholders = ", ".join(["?"] * len(remaining_flag_ids))
                cleared = conn.execute(
                    f"DELETE FROM source_block WHERE id IN ({placeholders}) RETURNING id",
                    remaining_flag_ids,
                ).fetchall()
                if cleared:
                    click.echo(f"[ok] {len(cleared)} flag(s) cleared from source_block")
        else:
            click.echo(f"[error] {result.get('message', 'Ingest failed')}")
    finally:
        os.unlink(corrections_path)


@flagged_cmd.command("open")
@click.argument("table")
@click.option("--key", default=None, help="Override primary key column.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def flagged_open(table, key, sources_config, pipeline_db):
    """Export blocked rows to incoming_dir and open the file for editing.

    The export is named to match the source pattern so vp ingest picks
    it up naturally after editing (e.g. van_flagged_2026-04-02.csv
    matches the van_*.csv pattern). After saving, run:

        vp ingest   or   vp flagged retry <table>

    \b
    Examples:
      vp flagged open van
      vp flagged open van --key van_id
    """
    from proto_pipe.pipelines.flagging import build_source_flag_export

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    incoming_dir = config_path_or_override("incoming_dir")
    pk_col = key or _resolve_primary_key(table, sources_config)

    # Check for existing export first
    path = _glob_flagged_export(table, incoming_dir)

    if not path:
        conn = duckdb.connect(p_db)
        try:
            if pk_col:
                df = coerce_for_display(build_source_flag_export(conn, table, pk_col))
            else:
                df = _get_raw_source_block(conn, table)

            if df.empty:
                click.echo(f"  No blocked rows for '{table}' — nothing to open.")
                return

            source = _resolve_source(table, sources_config)
            source_name = source["name"] if source else table
            output_path = _flagged_export_path(table, source_name, incoming_dir)
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
            click.echo(f"[ok] {len(df)} row(s) exported to: {output_path}")
            path = Path(output_path)
        finally:
            conn.close()

    click.echo(f"  Opening: {path}")
    click.launch(str(path))
    click.echo(f"\n  Edit and save, then run:")
    click.echo(f"    vp flagged retry {table}   — or —   vp ingest")


@flagged_cmd.command("clear")
@click.option("--table", required=True, help="Table to clear flags for.")
@click.option("--check", default=None, help="Only clear flags for this check name.")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def flagged_clear(table, check, yes, pipeline_db):
    """Clear blocked rows for a table without applying corrections.

    Use this when you have reviewed the flags and decided to proceed
    without making corrections. This cannot be undone.

    \b
    Examples:
      vp flagged clear --table van
      vp flagged clear --table van --check duplicate_conflict --yes
    """
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        count_query = "SELECT count(*) FROM source_block WHERE table_name = ?"
        count_params = [table]
        if check:
            count_query += " AND check_name = ?"
            count_params.append(check)

        count = conn.execute(count_query, count_params).fetchone()[0]

        if count == 0:
            click.echo(f"  No blocked rows to clear for '{table}'.")
            return

        scope = f"check '{check}'" if check else "all checks"
        if not yes:
            try:
                click.confirm(
                    f"Clear {count} blocked row(s) for '{table}' ({scope})? "
                    f"This cannot be undone.",
                    abort=True,
                )
            except click.Abort:
                click.echo("\n  Cancelled.")
                return

        del_query = "DELETE FROM source_block WHERE table_name = ?"
        del_params = [table]
        if check:
            del_query += " AND check_name = ?"
            del_params.append(check)

        conn.execute(del_query, del_params)
        click.echo(f"[ok] {count} flag(s) cleared for '{table}' ({scope})")
    finally:
        conn.close()


@flagged_cmd.command("retry")
@click.argument("table")
@click.option("--key", default=None, help="Override primary key column.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
def flagged_retry(table, key, pipeline_db, sources_config):
    """Re-ingest the most recent flagged export for a table.

    Globs incoming_dir for the most recently modified flagged export,
    strips pipeline columns, and runs through the ingest path with upsert.

    Rows present in the export → upserted to source table, flag cleared.
    Rows deleted from the export → treated as skip, flag cleared.

    \b
    Examples:
      vp flagged retry van
      vp flagged retry van --key van_id
    """
    from proto_pipe.io.config import SourceConfig
    from proto_pipe.io.ingest import ingest_single_file

    incoming_dir = config_path_or_override("incoming_dir")
    path = _glob_flagged_export(table, incoming_dir)

    if not path:
        click.echo(
            f"[error] No flagged export found for '{table}' in {incoming_dir}.\n"
            f"  Run: vp flagged open {table}"
        )
        return

    pk_col = key or _resolve_primary_key(table, sources_config)
    if not pk_col:
        return

    src_cfg = config_path_or_override("sources_config", sources_config)
    try:
        source = SourceConfig(src_cfg).get_by_table(table)
    except Exception:
        source = None

    if not source:
        click.echo(f"[error] No source found for table '{table}' in sources_config.yaml")
        return

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    click.echo(f"  Applying corrections from: {path.name}")

    conn = duckdb.connect(p_db)
    try:
        # Read export to find deleted rows (user wants to skip)
        import csv as _csv
        try:
            with open(path, newline="") as _f:
                reader = _csv.DictReader(_f)
                export_rows = list(reader)
            export_flag_ids = {
                row["_flag_id"] for row in export_rows
                if "_flag_id" in row and row["_flag_id"]
            }
        except Exception:
            export_flag_ids = set()

        # Find flags for this table that are NOT in the export (deleted = skip)
        all_flags = conn.execute(
            "SELECT id FROM source_block WHERE table_name = ?", [table]
        ).df()["id"].tolist()

        deleted_flag_ids = [fid for fid in all_flags if fid not in export_flag_ids]
        if deleted_flag_ids:
            placeholders = ", ".join(["?"] * len(deleted_flag_ids))
            conn.execute(
                f"DELETE FROM source_block WHERE id IN ({placeholders})",
                deleted_flag_ids,
            )
            from proto_pipe.io.db import log_ingest_state
            log_ingest_state(
                conn, path.name, table, "skipped",
                message=f"{len(deleted_flag_ids)} row(s) deleted from export — skipped"
            )
            click.echo(f"  [ok] {len(deleted_flag_ids)} deleted row(s) skipped and cleared")

        # Re-ingest the export with upsert + strip pipeline cols
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

        # Clear remaining flags for this table
        cleared = conn.execute(
            "DELETE FROM source_block WHERE table_name = ? RETURNING id",
            [table],
        ).fetchall()
        if cleared:
            click.echo(f"[ok] {len(cleared)} flag(s) cleared from source_block")

        if result.get("new_cols"):
            click.echo(f"  New columns added: {', '.join(result['new_cols'])}")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp validated (group)
# ---------------------------------------------------------------------------

@click.group(
    "validated",
    invoke_without_command=True,
    context_settings={"max_content_width": 120},
)
@click.option("--table", default=None, help="Filter by report table name.")
@click.option("--report", default=None, help="Filter by report name.")
@click.option(
    "--export",
    default=None,
    type=click.Choice(["csv", "term"]),
    help="Export format: csv writes a file, term prints to terminal.",
)
@click.option("--limit", default=500, show_default=True, help="Max rows to display.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.pass_context
def validated_cmd(ctx, table, report, export, limit, pipeline_db):
    """View validation failures (check/transform failures from vp validate).

    These warn but do not block deliverables. Correction path:
    vp validated edit → fix values → vp validated retry

    \b
    Examples:
      vp validated
      vp validated --report van_report
      vp validated --table van_report
      vp validated --export csv
    """
    if ctx.invoked_subcommand is not None:
        return

    from proto_pipe.cli.commands.table import get_reviewer

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)

    try:
        try:
            df = _get_raw_validation_block(conn, table, report, limit=limit)
        except Exception as e:
            click.echo(f"[error] Could not read validation_block: {e}")
            click.echo("Has 'vp db-init' been run yet?")
            return

        if df.empty:
            parts = []
            if report:
                parts.append(f"report '{report}'")
            if table:
                parts.append(f"table '{table}'")
            scope = " / ".join(parts) if parts else "any report"
            click.echo(f"\n  No validation failures for {scope} — all clear.")
            return

        parts = []
        if report:
            parts.append(report)
        if table:
            parts.append(table)
        label = " / ".join(parts) if parts else "all reports"
        title = f"validation_block — {label} ({len(df)} rows)"

        if export == "csv":
            from proto_pipe.reports.corrections import dated_export_path
            out_dir = config_path_or_override("output_dir")
            label_safe = (report or table or "all").replace(" ", "_")
            output_path = dated_export_path(out_dir, f"validation_block_{label_safe}")
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
            click.echo(f"[ok] {len(df)} row(s) exported to: {output_path}")
        else:
            reviewer = get_reviewer(edit=False)
            reviewer.show(df, title=title)

    finally:
        conn.close()


@validated_cmd.command("edit")
@click.option("--report", required=True, help="Report name to review and edit.")
@click.option("--key", default=None, help="Override primary key column.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def validated_edit(report, key, sources_config, pipeline_db):
    """Open validation failures in an enriched editable TUI view.

    Joins validation_block to the report table so you see the actual
    bad values. Edit inline and save — changes are upserted to the report
    table and flags cleared.

    \b
    Example:
      vp validated edit --report van_report
    """
    from proto_pipe.cli.commands.table import get_reviewer
    from proto_pipe.pipelines.flagging import build_validation_flag_export
    from proto_pipe.io.config import ReportConfig

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    rep_cfg = config_path_or_override("reports_config", sources_config)

    # Resolve report table name and PK
    try:
        report_config = ReportConfig(rep_cfg).get(report)
        table = report_config.get("target_table", report) if report_config else report
        source_table = report_config.get("source", {}).get("table") if report_config else None
    except Exception:
        table = report
        source_table = None

    pk_col = key
    if not pk_col and report_config:
        pk_col = report_config.get("source", {}).get("primary_key")
    if not pk_col and source_table:
        pk_col = _resolve_primary_key(source_table, sources_config)
    if not pk_col:
        click.echo(
            f"[error] Cannot build enriched view without a primary key.\n"
            f"  Use --key to specify the primary key column."
        )
        return

    conn = duckdb.connect(p_db)
    try:
        df = coerce_for_display(build_validation_flag_export(conn, table, report, pk_col))

        if df.empty:
            click.echo(f"\n  No validation failures for report '{report}' — all clear.")
            return

        title = f"validation_block — {report} ({len(df)} rows, enriched)"
        reviewer = get_reviewer(edit=True)
        edited_df = reviewer.edit(df, title=title, pk_col=pk_col)

        if edited_df is None or edited_df.equals(df):
            click.echo("  No changes made.")
            return

        _apply_validation_edits(conn, table, report, pk_col, df, edited_df)

    except Exception as e:
        click.echo(f"[error] Could not build enriched view: {e}")
        click.echo("  Run 'vp validated --report' for the raw failures view.")
    finally:
        conn.close()


def _apply_validation_edits(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    report: str,
    pk_col: str,
    original_df: pd.DataFrame,
    edited_df: pd.DataFrame,
) -> None:
    """Upsert edited rows back to the report table and clear resolved flags."""
    # Handle deleted rows (user chose to skip)
    orig_flag_ids = set(original_df["_flag_id"].dropna().tolist())
    edited_flag_ids = set(edited_df["_flag_id"].dropna().tolist()) if "_flag_id" in edited_df.columns else set()
    deleted_flag_ids = orig_flag_ids - edited_flag_ids

    if deleted_flag_ids:
        placeholders = ", ".join(["?"] * len(deleted_flag_ids))
        conn.execute(
            f"DELETE FROM validation_block WHERE id IN ({placeholders})",
            list(deleted_flag_ids),
        )
        click.echo(f"  [ok] {len(deleted_flag_ids)} row(s) skipped and cleared from validation_block")

    # Upsert remaining rows to report table
    data_cols = [c for c in edited_df.columns if not c.startswith("_")]
    corrections_df = edited_df[data_cols].copy()

    if corrections_df.empty or pk_col not in corrections_df.columns:
        return

    # Upsert via staging table
    conn.execute(
        "CREATE TEMP TABLE IF NOT EXISTS _val_corrections AS SELECT * FROM corrections_df LIMIT 0"
    )
    conn.execute("DELETE FROM _val_corrections")
    conn.execute("INSERT INTO _val_corrections SELECT * FROM corrections_df")

    update_cols = [c for c in data_cols if c != pk_col]
    if not update_cols:
        return

    set_clause = ", ".join(f'"{c}" = _val_corrections."{c}"' for c in update_cols)
    conn.execute(f"""
        UPDATE "{table}"
        SET {set_clause}
        FROM _val_corrections
        WHERE "{table}"."{pk_col}" = _val_corrections."{pk_col}"
    """)
    conn.execute("DROP TABLE IF EXISTS _val_corrections")

    click.echo(f"[ok] {len(corrections_df)} row(s) updated in '{table}'")

    # Clear resolved flags
    remaining_flag_ids = list(edited_flag_ids)
    if remaining_flag_ids:
        placeholders = ", ".join(["?"] * len(remaining_flag_ids))
        cleared = conn.execute(
            f"DELETE FROM validation_block WHERE id IN ({placeholders}) RETURNING id",
            remaining_flag_ids,
        ).fetchall()
        if cleared:
            click.echo(f"[ok] {len(cleared)} flag(s) cleared from validation_block")


@validated_cmd.command("open")
@click.argument("report")
@click.option("--key", default=None, help="Override primary key column.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def validated_open(report, key, sources_config, pipeline_db):
    """Export validation failures to output_dir and open for external editing.

    After editing and saving, run:
        vp validated retry <report>

    \b
    Example:
      vp validated open van_report
    """
    from proto_pipe.pipelines.flagging import build_validation_flag_export
    from proto_pipe.reports.corrections import dated_export_path
    from proto_pipe.io.config import ReportConfig

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    out_dir = config_path_or_override("output_dir")
    rep_cfg = config_path_or_override("reports_config", sources_config)

    try:
        report_config = ReportConfig(rep_cfg).get(report)
        table = report_config.get("target_table", report) if report_config else report
        source_table = report_config.get("source", {}).get("table") if report_config else None
    except Exception:
        table = report
        source_table = None

    pk_col = key
    if not pk_col and report_config:
        # Read primary_key from the report's source config — this is the
        # canonical location for report-layer primary keys (reports_config.yaml).
        pk_col = report_config.get("source", {}).get("primary_key")
    if not pk_col and source_table:
        # Fall back to sources_config.yaml if not set on the report source.
        pk_col = _resolve_primary_key(source_table, sources_config)
    if not pk_col:
        click.echo("[error] Cannot export without a primary key. Use --key.")
        return

    conn = duckdb.connect(p_db)
    try:
        df = coerce_for_display(build_validation_flag_export(conn, table, report, pk_col))
        if df.empty:
            click.echo(f"  No validation failures for '{report}' — nothing to open.")
            return

        output_path = dated_export_path(out_dir, f"validated_{report}")
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        click.echo(f"[ok] {len(df)} row(s) exported to: {output_path}")
    finally:
        conn.close()

    click.echo(f"  Opening: {output_path}")
    click.launch(output_path)
    click.echo(f"\n  Edit and save, then run:")
    click.echo(f"    vp validated retry {report}")


@validated_cmd.command("clear")
@click.option("--report", required=True, help="Report name to clear failures for.")
@click.option("--check", default=None, help="Only clear failures for this check name.")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def validated_clear(report, check, yes, pipeline_db):
    """Clear validation failures for a report without correcting them.

    \b
    Examples:
      vp validated clear --report van_report
      vp validated clear --report van_report --check null_check --yes
    """
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        count_query = "SELECT count(*) FROM validation_block WHERE report_name = ?"
        count_params = [report]
        if check:
            count_query += " AND check_name = ?"
            count_params.append(check)

        count = conn.execute(count_query, count_params).fetchone()[0]

        if count == 0:
            click.echo(f"  No validation failures to clear for '{report}'.")
            return

        scope = f"check '{check}'" if check else "all checks"
        if not yes:
            try:
                click.confirm(
                    f"Clear {count} failure(s) for '{report}' ({scope})? "
                    f"This cannot be undone.",
                    abort=True,
                )
            except click.Abort:
                click.echo("\n  Cancelled.")
                return

        del_query = "DELETE FROM validation_block WHERE report_name = ?"
        del_params = [report]
        if check:
            del_query += " AND check_name = ?"
            del_params.append(check)

        conn.execute(del_query, del_params)
        click.echo(f"[ok] {count} failure(s) cleared for '{report}' ({scope})")
    finally:
        conn.close()


@validated_cmd.command("retry")
@click.argument("report")
@click.option("--key", default=None, help="Override primary key column.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def validated_retry(report, key, sources_config, pipeline_db):
    """Apply corrected validation export back to the report table.

    Reads the most recently exported validated_<report>_*.csv from output_dir,
    strips flag columns, upserts corrected rows to the report table, and
    clears resolved flags. Rows deleted from the export are skipped.

    \b
    Example:
      vp validated retry van_report
    """
    from proto_pipe.reports.corrections import dated_export_path
    from proto_pipe.io.config import ReportConfig
    import csv as _csv
    import glob

    out_dir = config_path_or_override("output_dir")
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    rep_cfg = config_path_or_override("reports_config", sources_config)

    # Find most recently modified export for this report
    candidates = sorted(
        Path(out_dir).glob(f"validated_{report}_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        click.echo(
            f"[error] No export found for '{report}' in {out_dir}.\n"
            f"  Run: vp validated open {report}"
        )
        return

    path = candidates[0]
    click.echo(f"  Applying corrections from: {path.name}")

    try:
        report_config = ReportConfig(rep_cfg).get(report)
        table = report_config.get("target_table", report) if report_config else report
        source_table = report_config.get("source", {}).get("table") if report_config else None
    except Exception:
        table = report
        source_table = None

    pk_col = key
    if not pk_col and source_table:
        pk_col = _resolve_primary_key(source_table, sources_config)
    if not pk_col:
        click.echo("[error] Cannot apply corrections without a primary key. Use --key.")
        return

    conn = duckdb.connect(p_db)
    try:
        # Read export to find deleted rows
        try:
            with open(path, newline="") as _f:
                reader = _csv.DictReader(_f)
                export_rows = list(reader)
            export_flag_ids = {
                row["_flag_id"] for row in export_rows
                if "_flag_id" in row and row["_flag_id"]
            }
        except Exception:
            export_flag_ids = set()

        # Handle deleted rows → skip
        all_flags = conn.execute(
            "SELECT id FROM validation_block WHERE report_name = ?", [report]
        ).df()["id"].tolist()

        deleted_flag_ids = [fid for fid in all_flags if fid not in export_flag_ids]
        if deleted_flag_ids:
            placeholders = ", ".join(["?"] * len(deleted_flag_ids))
            conn.execute(
                f"DELETE FROM validation_block WHERE id IN ({placeholders})",
                deleted_flag_ids,
            )
            click.echo(f"  [ok] {len(deleted_flag_ids)} deleted row(s) skipped and cleared")

        # Load export, strip flag cols, upsert to report table
        corrections_df = pd.read_csv(path)
        data_cols = [c for c in corrections_df.columns if not c.startswith("_")]
        corrections_df = corrections_df[data_cols]

        if corrections_df.empty or pk_col not in corrections_df.columns:
            click.echo("  No data to apply after stripping pipeline columns.")
            return

        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS _val_retry AS SELECT * FROM corrections_df LIMIT 0"
        )
        conn.execute("DELETE FROM _val_retry")
        conn.execute("INSERT INTO _val_retry SELECT * FROM corrections_df")

        update_cols = [c for c in data_cols if c != pk_col]
        if update_cols:
            set_clause = ", ".join(f'"{c}" = _val_retry."{c}"' for c in update_cols)
            conn.execute(f"""
                UPDATE "{table}"
                SET {set_clause}
                FROM _val_retry
                WHERE "{table}"."{pk_col}" = _val_retry."{pk_col}"
            """)
        conn.execute("DROP TABLE IF EXISTS _val_retry")
        click.echo(f"[ok] {len(corrections_df)} row(s) applied to '{table}'")

        # Clear remaining flags
        cleared = conn.execute(
            "DELETE FROM validation_block WHERE report_name = ? RETURNING id",
            [report],
        ).fetchall()
        if cleared:
            click.echo(f"[ok] {len(cleared)} flag(s) cleared from validation_block")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Top-level: check-null-overwrites
# ---------------------------------------------------------------------------

@click.command("check-null-overwrites")
@click.option("--table", required=True, help="Table to scan for duplicate conflicts.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
def check_null_overwrites_cmd(table, pipeline_db, sources_config):
    """Scan a table for rows with the same primary key but different content.

    \b
    Example:
      vp check-null-overwrites --table van
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


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def flagged_commands(cli: click.Group) -> None:
    cli.add_command(flagged_cmd)
    cli.add_command(validated_cmd)
    cli.add_command(check_null_overwrites_cmd)
