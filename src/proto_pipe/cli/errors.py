"""Error commands — vp errors, vp errors source, vp errors report.

Replaces: vp flagged, vp validated, vp export validation.

vp errors              — summary counts both stages
vp errors source       — source_block rows grouped by cause, prescriptive fixes
vp errors source <n>   — one table's source_block rows
vp errors source export [name] [--open] — export enriched view
vp errors source edit <name>   — TUI inline edit
vp errors source clear [name]  — drop flags without fixing
vp errors source retry <name>  — re-ingest corrected export

vp errors report       — validation_block rows grouped by cause
vp errors report <n>   — one report's validation_block rows
vp errors report export [name] [--open] — export to Excel (Detail + Summary)
vp errors report edit <name>   — TUI inline edit
vp errors report clear [name]  — drop flags without fixing
vp errors report retry <name>  — upsert corrected export
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
# StageGroup — allows `vp errors source <name>` without subcommand collision
# ---------------------------------------------------------------------------

class StageGroup(click.Group):
    """Group where an unknown first token is treated as [name], not a subcommand."""

    def parse_args(self, ctx, args):
        if args and args[0] not in self.commands and not args[0].startswith("-"):
            ctx.ensure_object(dict)
            ctx.obj["stage_name"] = args.pop(0)
        return super().parse_args(ctx, args)


# ---------------------------------------------------------------------------
# Shared helpers (CLI-layer wiring — no business logic)
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


def _resolve_report_context(
    report: str,
    key: str | None,
    sources_config: str | None,
) -> tuple[str, str | None]:
    """Resolve (target_table, pk_col) for a report name."""
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


def _flagged_export_path(table: str, source_name: str, incoming_dir: str) -> str:
    """Build the export path for a source error export.

    Named <source_name>_flagged_<date>.csv so it matches the source pattern
    and is picked up by vp errors source retry automatically.
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




# ---------------------------------------------------------------------------
# vp errors (top-level group)
# ---------------------------------------------------------------------------

@click.group(
    "errors",
    invoke_without_command=True,
    context_settings={"max_content_width": 120},
)
@click.pass_context
def errors_cmd(ctx):
    """View and resolve pipeline errors.

    Shows a summary of errors across both stages (source ingest and report
    validation). Use subcommands to drill down into a specific stage.

    \b
    Examples:
      vp errors                     — summary counts both stages
      vp errors source              — all source errors, grouped by cause
      vp errors source van          — one table's errors
      vp errors source export van   — export for editing
      vp errors report              — all validation errors
      vp errors report van_report   — one report's errors
    """
    if ctx.invoked_subcommand is not None:
        return

    from proto_pipe.pipelines.query import query_error_overview
    from proto_pipe.cli.prompts import print_error_overview

    p_db = config_path_or_override("pipeline_db")
    conn = duckdb.connect(p_db)
    try:
        overview = query_error_overview(conn)
        print_error_overview(overview)
    finally:
        conn.close()


# ===========================================================================
# vp errors source
# ===========================================================================

@errors_cmd.group(
    "source",
    cls=StageGroup,
    invoke_without_command=True,
    context_settings={"max_content_width": 120},
)
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.pass_context
def source_cmd(ctx, pipeline_db):
    """View source-level ingest errors (type conflicts, duplicates).

    \b
    Examples:
      vp errors source
      vp errors source van
    """
    if ctx.invoked_subcommand is not None:
        return

    from proto_pipe.pipelines.query import query_error_groups, query_file_failures
    from proto_pipe.cli.prompts import print_prescriptive_errors, print_file_failures

    name = (ctx.obj or {}).get("stage_name")
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        file_failures = query_file_failures(conn, name=name)
        by_scope = query_error_groups(conn, "source", name=name)

        if not file_failures and not by_scope:
            scope = f"'{name}'" if name else "any source"
            click.echo(f"\n  No source errors for {scope} — all clear.")
            return

        print_file_failures(file_failures)
        if by_scope:
            print_prescriptive_errors("source", name, by_scope)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp errors source export [name]
# ---------------------------------------------------------------------------

@source_cmd.command("export")
@click.argument("name", required=False, default=None)
@click.option("--open", "open_file", is_flag=True, default=False, help="Open the file after exporting.")
@click.option("--key", default=None, help="Override primary key column.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def source_export(name, open_file, key, sources_config, pipeline_db):
    """Export source errors to a CSV for editing.

    Exported to incoming_dir so retry picks it up. Use --open to launch the
    file in your default editor after export.

    \b
    Examples:
      vp errors source export van
      vp errors source export van --open
    """
    from proto_pipe.pipelines.flagging import build_source_flag_export, get_raw_flags

    if not name:
        click.echo("[error] Specify a table name: vp errors source export <name>")
        return

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    incoming_dir = config_path_or_override("incoming_dir")
    pk_col = key or _resolve_primary_key(name, sources_config)

    conn = duckdb.connect(p_db)
    try:
        if pk_col:
            df = coerce_for_display(build_source_flag_export(conn, name, pk_col))
        else:
            df = get_raw_flags(conn, "source_block", filters={"table_name": name})

        if df.empty:
            click.echo(f"  No source errors for '{name}' — nothing to export.")
            return

        source = _resolve_source(name, sources_config)
        source_name = source["name"] if source else name
        output_path = _flagged_export_path(name, source_name, incoming_dir)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        click.echo(f"[ok] {len(df)} row(s) exported to: {output_path}")
    finally:
        conn.close()

    if open_file:
        click.echo(f"  Opening: {output_path}")
        click.launch(output_path)

    click.echo(f"\n  Edit and save, then run:")
    click.echo(f"    vp errors source retry {name}")


# ---------------------------------------------------------------------------
# vp errors source edit <name>
# ---------------------------------------------------------------------------

@source_cmd.command("edit")
@click.argument("name", required=True)
@click.option("--key", default=None, help="Override primary key column.")
@click.option(
    "--resolve",
    is_flag=True,
    default=False,
    help="Wizard mode — step through each conflict for column-by-column resolution.",
)
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def source_edit(name, key, resolve, sources_config, pipeline_db):
    """Open source errors in an enriched editable TUI view.

    Joins source_block to source files so you see the actual bad values
    alongside the flag reason. Edit inline and save.

    \b
    Examples:
      vp errors source edit van
      vp errors source edit van --resolve
    """
    from proto_pipe.cli.commands.table import get_reviewer
    from proto_pipe.pipelines.flagging import build_source_flag_export, clear_flags

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    pk_col = key or _resolve_primary_key(name, sources_config)
    if not pk_col:
        click.echo(
            f"[error] Cannot build enriched view without a primary key for '{name}'.\n"
            f"  Set primary_key in sources_config.yaml or use --key."
        )
        return

    conn = duckdb.connect(p_db)
    try:
        df = coerce_for_display(build_source_flag_export(conn, name, pk_col))

        if df.empty:
            click.echo(f"\n  No source errors for '{name}' — all clear.")
            return

        title = f"source_block — {name} ({len(df)} rows, enriched)"

        if resolve:
            _run_resolve_wizard(conn, df, name, pk_col)
            return

        reviewer = get_reviewer(edit=True)
        edited_df = reviewer.edit(df, title=title, pk_col=pk_col)

        if edited_df is None or edited_df.equals(df):
            click.echo("  No changes made.")
            return

        deleted_ids, remaining_ids = _diff_flag_ids(df, edited_df)

        if deleted_ids:
            cleared = clear_flags(conn, "source_block", deleted_ids)
            click.echo(f"  [ok] {cleared} row(s) skipped and cleared from source_block")

        corrections_df = _strip_pipeline_cols(edited_df)
        if corrections_df.empty:
            return

        _ingest_corrections(conn, corrections_df, name, pk_col, sources_config)

        if remaining_ids:
            cleared = clear_flags(conn, "source_block", remaining_ids)
            if cleared:
                click.echo(f"[ok] {cleared} flag(s) cleared from source_block")

    except Exception as e:
        click.echo(f"[error] Could not build enriched view: {e}")
        click.echo("  Run 'vp errors source' for the grouped summary view.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp errors source clear [name]
# ---------------------------------------------------------------------------

@source_cmd.command("clear")
@click.argument("name", required=False, default=None)
@click.option("--check", default=None, help="Only clear flags for this check name.")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def source_clear(name, check, yes, pipeline_db):
    """Clear source errors without applying corrections.

    This cannot be undone. Omit name to clear all tables.

    \b
    Examples:
      vp errors source clear van
      vp errors source clear van --check duplicate_conflict --yes
      vp errors source clear --yes
    """
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        count_query = "SELECT count(*) FROM source_block WHERE 1=1"
        count_params: list = []
        if name:
            count_query += " AND table_name = ?"
            count_params.append(name)
        if check:
            count_query += " AND check_name = ?"
            count_params.append(check)

        count = conn.execute(count_query, count_params).fetchone()[0]

        if count == 0:
            scope_desc = f"'{name}'" if name else "any table"
            click.echo(f"  No source errors to clear for {scope_desc}.")
            return

        table_desc = f"'{name}'" if name else "all tables"
        scope = f"check '{check}'" if check else "all checks"
        if not yes:
            try:
                click.confirm(
                    f"Clear {count} source error(s) for {table_desc} ({scope})? "
                    f"This cannot be undone.",
                    abort=True,
                )
            except click.Abort:
                click.echo("\n  Cancelled.")
                return

        del_query = "DELETE FROM source_block WHERE 1=1"
        del_params: list = []
        if name:
            del_query += " AND table_name = ?"
            del_params.append(name)
        if check:
            del_query += " AND check_name = ?"
            del_params.append(check)

        conn.execute(del_query, del_params)
        click.echo(f"[ok] {count} flag(s) cleared for {table_desc} ({scope})")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp errors source retry <name>
# ---------------------------------------------------------------------------

@source_cmd.command("retry")
@click.argument("name", required=True)
@click.option("--key", default=None, help="Override primary key column.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
def source_retry(name, key, pipeline_db, sources_config):
    """Re-ingest the most recent error export for a source table.

    Globs incoming_dir for the most recently modified export, strips
    pipeline columns, and runs through the ingest path with upsert.

    Rows present in the export → upserted, flag cleared.
    Rows deleted from the export → treated as skip, flag cleared.

    \b
    Examples:
      vp errors source retry van
      vp errors source retry van --key van_id
    """
    from proto_pipe.cli.scaffold import glob_most_recent
    from proto_pipe.io.config import SourceConfig
    from proto_pipe.io.ingest import ingest_single_file
    from proto_pipe.pipelines.flagging import clear_flags

    incoming_dir = config_path_or_override("incoming_dir")
    path = glob_most_recent(
        incoming_dir,
        f"*{name}*_flagged_*.csv",
        f"{name}_flagged_*.csv",
    )

    if not path:
        click.echo(
            f"[error] No error export found for '{name}' in {incoming_dir}.\n"
            f"  Run: vp errors source export {name}"
        )
        return

    pk_col = key or _resolve_primary_key(name, sources_config)
    if not pk_col:
        return

    src_cfg = config_path_or_override("sources_config", sources_config)
    try:
        source = SourceConfig(src_cfg).get_by_table(name)
    except Exception:
        source = None

    if not source:
        click.echo(f"[error] No source found for table '{name}' in sources_config.yaml")
        return

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    click.echo(f"  Applying corrections from: {path.name}")

    conn = duckdb.connect(p_db)
    try:
        # Handle deleted rows (removed from export = skip, clear flag)
        export_flag_ids = _read_export_flag_ids(path)
        all_flag_ids = set(
            conn.execute(
                "SELECT id FROM source_block WHERE table_name = ?", [name]
            ).df()["id"].tolist()
        )
        deleted_ids = all_flag_ids - export_flag_ids
        if deleted_ids:
            cleared = clear_flags(conn, "source_block", deleted_ids)
            from proto_pipe.io.db import log_ingest_state
            log_ingest_state(
                conn, path.name, name, "skipped",
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

        click.echo(f"[ok] {result['rows']} row(s) applied to '{name}'")

        # Clear remaining flags for this table
        remaining = conn.execute(
            "DELETE FROM source_block WHERE table_name = ? RETURNING id",
            [name],
        ).fetchall()
        if remaining:
            click.echo(f"[ok] {len(remaining)} flag(s) cleared from source_block")

        if result.get("new_cols"):
            click.echo(f"  New columns added: {', '.join(result['new_cols'])}")

    finally:
        conn.close()


# ===========================================================================
# vp errors report
# ===========================================================================

@errors_cmd.group(
    "report",
    cls=StageGroup,
    invoke_without_command=True,
    context_settings={"max_content_width": 120},
)
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.pass_context
def report_cmd(ctx, pipeline_db):
    """View report-level validation errors (check/transform failures).

    These warn but do not block deliverables. Correction path:
    export → fix values → retry, or edit check function → revalidate.

    \b
    Examples:
      vp errors report
      vp errors report van_report
    """
    if ctx.invoked_subcommand is not None:
        return

    from proto_pipe.pipelines.query import query_error_groups
    from proto_pipe.cli.prompts import print_prescriptive_errors

    name = (ctx.obj or {}).get("stage_name")
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        by_scope = query_error_groups(conn, "report", name=name)
        print_prescriptive_errors("report", name, by_scope)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp errors report export [name]
# ---------------------------------------------------------------------------

@report_cmd.command("export")
@click.argument("name", required=False, default=None)
@click.option("--open", "open_file", is_flag=True, default=False, help="Open the file after exporting.")
@click.option("--key", default=None, help="Override primary key column.")
@click.option("--output", default=None, help="Override output path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def report_export(name, open_file, key, output, sources_config, pipeline_db):
    """Export validation errors to an Excel file (Detail + Summary sheets).

    Detail sheet: one row per failed record, joined to the report table.
    Summary sheet: one row per check with total failure counts.

    \b
    Examples:
      vp errors report export van_report
      vp errors report export van_report --open
      vp errors report export --output /tmp/validation.xlsx
    """
    from datetime import date
    from proto_pipe.pipelines.flagging import build_validation_flag_export
    from proto_pipe.io.config import config_path_or_override as _cfg

    p_db = _cfg("pipeline_db", pipeline_db)
    out_dir = _cfg("output_dir")
    src_cfg = _cfg("sources_config", sources_config)

    if output:
        output_path = output
    else:
        today = date.today().isoformat()
        stem = f"validation_{name}_{today}" if name else f"validation_{today}"
        output_path = str(Path(out_dir) / f"{stem}.xlsx")

    conn = duckdb.connect(p_db)
    try:
        # Resolve which reports to export
        if name:
            report_names = [name]
        else:
            report_names = conn.execute(
                "SELECT DISTINCT report_name FROM validation_block ORDER BY report_name"
            ).df()["report_name"].tolist()

        if not report_names:
            click.echo("[error] No validation errors found.")
            return

        # Resolve pk_col per report from sources_config
        try:
            from proto_pipe.io.config import SourceConfig, ReportConfig
            rep_cfg_path = _cfg("reports_config")
            rep_config_obj = ReportConfig(rep_cfg_path)
            src_config_obj = SourceConfig(src_cfg)
        except Exception:
            rep_config_obj = None
            src_config_obj = None

        detail_frames = []
        for rname in report_names:
            table = rname
            pk_col = key
            if rep_config_obj and not pk_col:
                try:
                    rcfg = rep_config_obj.get(rname)
                    table = rcfg.get("target_table", rname) if rcfg else rname
                    source_table = rcfg.get("source", {}).get("table") if rcfg else None
                    if source_table and src_config_obj:
                        src = src_config_obj.get_by_table(source_table)
                        pk_col = src.get("primary_key") if src else None
                except Exception:
                    pass

            if not pk_col:
                df = coerce_for_display(conn.execute("""
                    SELECT
                        id          AS _flag_id,
                        report_name,
                        check_name  AS _flag_check,
                        pk_value,
                        bad_columns AS _flag_columns,
                        reason      AS _flag_reason,
                        flagged_at
                    FROM validation_block
                    WHERE report_name = ?
                    ORDER BY flagged_at DESC
                """, [rname]).df())
            else:
                try:
                    df = coerce_for_display(
                        build_validation_flag_export(conn, table, rname, pk_col)
                    )
                except Exception:
                    df = coerce_for_display(conn.execute("""
                        SELECT * FROM validation_block WHERE report_name = ?
                        ORDER BY flagged_at DESC
                    """, [rname]).df())

            if not df.empty:
                detail_frames.append(df)

        detail_df = pd.concat(detail_frames, ignore_index=True) if detail_frames else pd.DataFrame()

        # Summary sheet
        summary_df = coerce_for_display(conn.execute("""
            SELECT
                report_name,
                check_name,
                count(*) AS failure_count,
                min(flagged_at) AS first_flagged,
                max(flagged_at) AS last_flagged
            FROM validation_block
            WHERE 1=1
            {}
            GROUP BY report_name, check_name
            ORDER BY report_name, check_name
        """.format("AND report_name = ?" if name else ""),
            [name] if name else []
        ).df())

        if detail_df.empty:
            scope = f" for report '{name}'" if name else ""
            click.echo(f"[error] No validation errors found{scope}.")
            return

        # Write two-sheet Excel
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        for df in [detail_df, summary_df]:
            for col in df.select_dtypes(include=["datetimetz"]).columns:
                df[col] = df[col].dt.tz_localize(None)

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            detail_df.to_excel(writer, sheet_name="Detail", index=False)
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

        detail_rows = len(detail_df)
        summary_rows = len(summary_df)
        click.echo(f"[ok] {detail_rows} failure(s) exported to: {output_path}")
        click.echo(f"  Detail: {detail_rows} row(s)  |  Summary: {summary_rows} check(s)")

    except ValueError as e:
        click.echo(f"[error] {e}")
    finally:
        conn.close()

    if open_file:
        click.echo(f"  Opening: {output_path}")
        click.launch(output_path)

    if name:
        click.echo(f"\n  After editing, run: vp errors report retry {name}")


# ---------------------------------------------------------------------------
# vp errors report edit <name>
# ---------------------------------------------------------------------------

@report_cmd.command("edit")
@click.argument("name", required=True)
@click.option("--key", default=None, help="Override primary key column.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def report_edit(name, key, sources_config, pipeline_db):
    """Open validation errors in an enriched editable TUI view.

    Joins validation_block to the report table so you see the actual
    bad values. Edit inline and save — changes are upserted and flags cleared.

    \b
    Example:
      vp errors report edit van_report
    """
    from proto_pipe.cli.commands.table import get_reviewer
    from proto_pipe.io.db import upsert_via_staging
    from proto_pipe.pipelines.flagging import build_validation_flag_export, clear_flags

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    table, pk_col = _resolve_report_context(name, key, sources_config)
    if not pk_col:
        click.echo(
            f"[error] Cannot build enriched view without a primary key.\n"
            f"  Use --key to specify the primary key column."
        )
        return

    conn = duckdb.connect(p_db)
    try:
        df = coerce_for_display(build_validation_flag_export(conn, table, name, pk_col))

        if df.empty:
            click.echo(f"\n  No validation errors for report '{name}' — all clear.")
            return

        title = f"validation_block — {name} ({len(df)} rows, enriched)"
        reviewer = get_reviewer(edit=True)
        edited_df = reviewer.edit(df, title=title, pk_col=pk_col)

        if edited_df is None or edited_df.equals(df):
            click.echo("  No changes made.")
            return

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
        click.echo("  Run 'vp errors report' for the grouped summary view.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp errors report clear [name]
# ---------------------------------------------------------------------------

@report_cmd.command("clear")
@click.argument("name", required=False, default=None)
@click.option("--check", default=None, help="Only clear failures for this check name.")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def report_clear(name, check, yes, pipeline_db):
    """Clear validation errors without correcting them.

    This cannot be undone. Omit name to clear all reports.

    \b
    Examples:
      vp errors report clear van_report
      vp errors report clear van_report --check null_check --yes
      vp errors report clear --yes
    """
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        count_query = "SELECT count(*) FROM validation_block WHERE 1=1"
        count_params: list = []
        if name:
            count_query += " AND report_name = ?"
            count_params.append(name)
        if check:
            count_query += " AND check_name = ?"
            count_params.append(check)

        count = conn.execute(count_query, count_params).fetchone()[0]

        if count == 0:
            scope_desc = f"'{name}'" if name else "all reports"
            click.echo(f"  No validation errors to clear for {scope_desc}.")
            return

        report_desc = f"'{name}'" if name else "all reports"
        scope = f"check '{check}'" if check else "all checks"
        if not yes:
            try:
                click.confirm(
                    f"Clear {count} validation error(s) for {report_desc} ({scope})? "
                    f"This cannot be undone.",
                    abort=True,
                )
            except click.Abort:
                click.echo("\n  Cancelled.")
                return

        del_query = "DELETE FROM validation_block WHERE 1=1"
        del_params: list = []
        if name:
            del_query += " AND report_name = ?"
            del_params.append(name)
        if check:
            del_query += " AND check_name = ?"
            del_params.append(check)

        conn.execute(del_query, del_params)
        click.echo(f"[ok] {count} failure(s) cleared for {report_desc} ({scope})")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp errors report retry <name>
# ---------------------------------------------------------------------------

@report_cmd.command("retry")
@click.argument("name", required=True)
@click.option("--key", default=None, help="Override primary key column.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def report_retry(name, key, sources_config, pipeline_db):
    """Apply corrected validation export back to the report table.

    Reads the most recently exported validated_<name>_*.csv from output_dir,
    strips flag columns, upserts corrected rows, and clears resolved flags.
    Rows deleted from the export are skipped.

    \b
    Example:
      vp errors report retry van_report
    """
    from proto_pipe.cli.scaffold import glob_most_recent
    from proto_pipe.io.db import upsert_via_staging
    from proto_pipe.pipelines.flagging import clear_flags

    out_dir = config_path_or_override("output_dir")
    p_db = config_path_or_override("pipeline_db", pipeline_db)

    path = glob_most_recent(out_dir, f"validated_{name}_*.csv")
    if not path:
        click.echo(
            f"[error] No export found for '{name}' in {out_dir}.\n"
            f"  Run: vp errors report export {name}"
        )
        return

    click.echo(f"  Applying corrections from: {path.name}")

    table, pk_col = _resolve_report_context(name, key, sources_config)
    if not pk_col:
        click.echo("[error] Cannot apply corrections without a primary key. Use --key.")
        return

    conn = duckdb.connect(p_db)
    try:
        # Handle deleted rows (removed from export = skip)
        export_flag_ids = _read_export_flag_ids(path)
        all_flag_ids = set(
            conn.execute(
                "SELECT id FROM validation_block WHERE report_name = ?", [name]
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
            [name],
        ).fetchall()
        if remaining:
            click.echo(f"[ok] {len(remaining)} flag(s) cleared from validation_block")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Top-level: check-null-overwrites (kept from flagged.py)
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
            click.echo(f"Run: vp errors source {table}")
        else:
            click.echo(f"[ok] No new conflicts found in '{table}'")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def errors_commands(cli: click.Group) -> None:
    cli.add_command(errors_cmd)
    cli.add_command(check_null_overwrites_cmd)
