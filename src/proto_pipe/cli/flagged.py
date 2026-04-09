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
# Shared helpers (CLI-layer wiring only — no business logic)
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


def _flagged_export_path(table: str, source_name: str, incoming_dir: str) -> str:
    """Build the export path for a flagged table export.

    Named <source_name>_flagged_<date>.csv so it matches the source pattern
    and is picked up by vp ingest / vp flagged retry automatically.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return str(Path(incoming_dir) / f"{source_name}_flagged_{today}.csv")


def _diff_flag_ids(
    original_df: pd.DataFrame,
    edited_df: pd.DataFrame,
) -> tuple[set[str], set[str]]:
    """Compute deleted and remaining flag IDs from original vs edited DataFrames."""
    orig_ids = set(original_df["_flag_id"].dropna().tolist())
    edited_ids = (
        set(edited_df["_flag_id"].dropna().tolist())
        if "_flag_id" in edited_df.columns
        else set()
    )
    return orig_ids - edited_ids, edited_ids


def _read_export_flag_ids(path: Path) -> set[str]:
    """Read _flag_id values from a CSV export. Returns empty set on error."""
    import csv as _csv
    try:
        with open(path, newline="") as _f:
            reader = _csv.DictReader(_f)
            return {
                row["_flag_id"]
                for row in reader
                if "_flag_id" in row and row["_flag_id"]
            }
    except Exception:
        return set()


def _strip_pipeline_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Return only non-underscore-prefixed columns."""
    data_cols = [c for c in df.columns if not c.startswith("_")]
    return df[data_cols].copy()


def _resolve_report_context(
    report: str,
    key: str | None,
    sources_config: str | None,
) -> tuple[str, str | None]:
    """Resolve (target_table, pk_col) for a report name.

    Reads ReportConfig to find target_table and primary_key. Falls back to
    sources_config.yaml for the PK if not set on the report.

    Returns (table, pk_col) where pk_col may be None if unresolvable.
    """
    from proto_pipe.io.config import ReportConfig

    rep_cfg = config_path_or_override("reports_config", sources_config)
    report_config = None
    source_table = None
    try:
        report_config = ReportConfig(rep_cfg).get(report)
        table = report_config.get("target_table", report) if report_config else report
        source_table = report_config.get("source", {}).get("table") if report_config else None
    except Exception:
        table = report

    pk_col = key
    if not pk_col and report_config:
        pk_col = report_config.get("source", {}).get("primary_key")
    if not pk_col and source_table:
        pk_col = _resolve_primary_key(source_table, sources_config)
    return table, pk_col


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
    from proto_pipe.pipelines.flagging import get_raw_flags

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)

    try:
        filters = {"table_name": table} if table else None
        df = get_raw_flags(conn, "source_block", filters=filters, limit=limit)

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
    from proto_pipe.pipelines.flagging import build_source_flag_export, clear_flags

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
        df = coerce_for_display(build_source_flag_export(conn, table, pk_col))

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

        # Diff flag IDs → clear deleted, apply edits, clear remaining
        deleted_ids, remaining_ids = _diff_flag_ids(df, edited_df)

        if deleted_ids:
            cleared = clear_flags(conn, "source_block", deleted_ids)
            click.echo(f"  [ok] {cleared} row(s) skipped and cleared from source_block")

        corrections_df = _strip_pipeline_cols(edited_df)
        if corrections_df.empty:
            return

        # Source corrections go through the full ingest path
        _ingest_corrections(conn, corrections_df, table, pk_col, sources_config)

        if remaining_ids:
            cleared = clear_flags(conn, "source_block", remaining_ids)
            if cleared:
                click.echo(f"[ok] {cleared} flag(s) cleared from source_block")

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
    from proto_pipe.pipelines.flagging import clear_flags

    for _, row in df.iterrows():
        pk_val = row.get(pk_col, "unknown")
        reason = row.get("_flag_reason", "")

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

        flag_id = row.get("_flag_id")
        if action in ("skip", "accept"):
            clear_flags(conn, "source_block", [flag_id])
            msg = "existing row kept" if action == "skip" else "re-ingest the source file to apply"
            click.echo(f"  [ok] Flag cleared — {msg}.")
        else:
            click.echo(f"  [skip] Left for later.")


def _ingest_corrections(
    conn: duckdb.DuckDBPyConnection,
    corrections_df: pd.DataFrame,
    table: str,
    pk_col: str,
    sources_config: str | None,
) -> None:
    """Write corrections through the full ingest path (source layer only)."""
    from proto_pipe.io.config import SourceConfig, config_path_or_override
    from proto_pipe.io.ingest import ingest_single_file
    import tempfile, os

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
    from proto_pipe.cli.scaffold import glob_most_recent
    from proto_pipe.pipelines.flagging import build_source_flag_export, get_raw_flags

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    incoming_dir = config_path_or_override("incoming_dir")
    pk_col = key or _resolve_primary_key(table, sources_config)

    # Check for existing export first
    path = glob_most_recent(
        incoming_dir,
        f"*{table}*_flagged_*.csv",
        f"{table}_flagged_*.csv",
    )

    if not path:
        conn = duckdb.connect(p_db)
        try:
            if pk_col:
                df = coerce_for_display(build_source_flag_export(conn, table, pk_col))
            else:
                df = get_raw_flags(conn, "source_block", filters={"table_name": table})

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
    from proto_pipe.cli.scaffold import glob_most_recent
    from proto_pipe.io.config import SourceConfig
    from proto_pipe.io.ingest import ingest_single_file
    from proto_pipe.pipelines.flagging import clear_flags

    incoming_dir = config_path_or_override("incoming_dir")
    path = glob_most_recent(
        incoming_dir,
        f"*{table}*_flagged_*.csv",
        f"{table}_flagged_*.csv",
    )

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
        # Handle deleted rows (removed from export = skip, clear flag)
        export_flag_ids = _read_export_flag_ids(path)
        all_flag_ids = set(
            conn.execute(
                "SELECT id FROM source_block WHERE table_name = ?", [table]
            ).df()["id"].tolist()
        )
        deleted_ids = all_flag_ids - export_flag_ids
        if deleted_ids:
            cleared = clear_flags(conn, "source_block", deleted_ids)
            from proto_pipe.io.db import log_ingest_state
            log_ingest_state(
                conn, path.name, table, "skipped",
                message=f"{cleared} row(s) deleted from export — skipped"
            )
            click.echo(f"  [ok] {cleared} deleted row(s) skipped and cleared")

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
        remaining = conn.execute(
            "DELETE FROM source_block WHERE table_name = ? RETURNING id",
            [table],
        ).fetchall()
        if remaining:
            click.echo(f"[ok] {len(remaining)} flag(s) cleared from source_block")

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
    from proto_pipe.pipelines.flagging import get_raw_flags

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)

    try:
        filters = {}
        if table:
            filters["table_name"] = table
        if report:
            filters["report_name"] = report

        try:
            df = get_raw_flags(
                conn, "validation_block",
                filters=filters or None,
                limit=limit,
            )
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
    from proto_pipe.io.db import upsert_via_staging
    from proto_pipe.pipelines.flagging import build_validation_flag_export, clear_flags

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    table, pk_col = _resolve_report_context(report, key, sources_config)
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

        # Diff flag IDs → clear deleted, upsert edits, clear remaining
        deleted_ids, remaining_ids = _diff_flag_ids(df, edited_df)

        if deleted_ids:
            cleared = clear_flags(conn, "validation_block", deleted_ids)
            click.echo(f"  [ok] {cleared} row(s) skipped and cleared from validation_block")

        corrections_df = _strip_pipeline_cols(edited_df)
        if not corrections_df.empty and pk_col in corrections_df.columns:
            upsert_via_staging(conn, table, corrections_df, pk_col)
            click.echo(f"[ok] {len(corrections_df)} row(s) updated in '{table}'")

        if remaining_ids:
            cleared = clear_flags(conn, "validation_block", remaining_ids)
            if cleared:
                click.echo(f"[ok] {cleared} flag(s) cleared from validation_block")

    except Exception as e:
        click.echo(f"[error] Could not build enriched view: {e}")
        click.echo("  Run 'vp validated --report' for the raw failures view.")
    finally:
        conn.close()


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

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    out_dir = config_path_or_override("output_dir")
    table, pk_col = _resolve_report_context(report, key, sources_config)
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
@click.option("--report", default=None, help="Report name to clear failures for. Omit to clear all reports.")
@click.option("--check", default=None, help="Only clear failures for this check name.")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def validated_clear(report, check, yes, pipeline_db):
    """Clear validation failures without correcting them.

    Omit --report to clear all reports. Scope with --check to target
    a specific check. Use --yes to skip the confirmation prompt.

    \b
    Examples:
      vp validated clear
      vp validated clear --report van_report
      vp validated clear --report van_report --check null_check --yes
    """
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        count_query = "SELECT count(*) FROM validation_block WHERE 1=1"
        count_params = []
        if report:
            count_query += " AND report_name = ?"
            count_params.append(report)
        if check:
            count_query += " AND check_name = ?"
            count_params.append(check)

        count = conn.execute(count_query, count_params).fetchone()[0]

        if count == 0:
            scope_desc = f"'{report}'" if report else "all reports"
            click.echo(f"  No validation failures to clear for {scope_desc}.")
            return

        report_desc = f"'{report}'" if report else "all reports"
        scope = f"check '{check}'" if check else "all checks"
        if not yes:
            try:
                click.confirm(
                    f"Clear {count} failure(s) for {report_desc} ({scope})? "
                    f"This cannot be undone.",
                    abort=True,
                )
            except click.Abort:
                click.echo("\n  Cancelled.")
                return

        del_query = "DELETE FROM validation_block WHERE 1=1"
        del_params = []
        if report:
            del_query += " AND report_name = ?"
            del_params.append(report)
        if check:
            del_query += " AND check_name = ?"
            del_params.append(check)

        conn.execute(del_query, del_params)
        click.echo(f"[ok] {count} failure(s) cleared for {report_desc} ({scope})")
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
    from proto_pipe.cli.scaffold import glob_most_recent
    from proto_pipe.io.db import upsert_via_staging
    from proto_pipe.pipelines.flagging import clear_flags

    out_dir = config_path_or_override("output_dir")
    p_db = config_path_or_override("pipeline_db", pipeline_db)

    path = glob_most_recent(out_dir, f"validated_{report}_*.csv")
    if not path:
        click.echo(
            f"[error] No export found for '{report}' in {out_dir}.\n"
            f"  Run: vp validated open {report}"
        )
        return

    click.echo(f"  Applying corrections from: {path.name}")

    table, pk_col = _resolve_report_context(report, key, sources_config)
    if not pk_col:
        click.echo("[error] Cannot apply corrections without a primary key. Use --key.")
        return

    conn = duckdb.connect(p_db)
    try:
        # Handle deleted rows (removed from export = skip)
        export_flag_ids = _read_export_flag_ids(path)
        all_flag_ids = set(
            conn.execute(
                "SELECT id FROM validation_block WHERE report_name = ?", [report]
            ).df()["id"].tolist()
        )
        deleted_ids = all_flag_ids - export_flag_ids
        if deleted_ids:
            cleared = clear_flags(conn, "validation_block", deleted_ids)
            click.echo(f"  [ok] {cleared} deleted row(s) skipped and cleared")

        # Load, strip flag cols, upsert
        corrections_df = _strip_pipeline_cols(pd.read_csv(path))

        if corrections_df.empty or pk_col not in corrections_df.columns:
            click.echo("  No data to apply after stripping pipeline columns.")
            return

        upsert_via_staging(conn, table, corrections_df, pk_col)
        click.echo(f"[ok] {len(corrections_df)} row(s) applied to '{table}'")

        # Clear remaining flags
        remaining = conn.execute(
            "DELETE FROM validation_block WHERE report_name = ? RETURNING id",
            [report],
        ).fetchall()
        if remaining:
            click.echo(f"[ok] {len(remaining)} flag(s) cleared from validation_block")

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
