"""Export commands — vp export validation, vp export log."""

from datetime import date, datetime, timezone

import click

from proto_pipe.io.config import config_path_or_override
from proto_pipe.pipelines.query import query_pipeline_events


# ---------------------------------------------------------------------------
# vp export
# ---------------------------------------------------------------------------

@click.group("export")
def export():
    """Export pipeline data to files."""
    pass


# ---------------------------------------------------------------------------
# vp export validation
# ---------------------------------------------------------------------------

@click.command("export-validation")
@click.option("--report",       default=None, help="Export failures for one report only.")
@click.option("--output",       default=None, help="Output path (.xlsx). Defaults to output_dir.")
@click.option("--pipeline-db",  default=None, help="Override pipeline DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
def export_validation(report, output, pipeline_db, sources_config):
    """Export validation failures to a two-sheet Excel file.

    Sheet 'Detail'  — one row per failed record, joined to the report table
                       so the actual bad values are visible alongside the
                       flag reason. Produced via build_validation_flag_export.
    Sheet 'Summary' — one row per check, with total failure counts.

    Validation failures are warnings — they do not block deliverables.
    Use this to identify which records need correction, then fix at source,
    re-ingest, and re-validate.

    \b
    Examples:
      vp export-validation
      vp export-validation --report van_report
      vp export-validation --output /tmp/validation.xlsx
    """
    from datetime import date
    from pathlib import Path
    import duckdb
    import pandas as pd

    from proto_pipe.pipelines.flagging import build_validation_flag_export
    from proto_pipe.io.config import config_path_or_override as _cfg
    from proto_pipe.io.db import coerce_for_display

    p_db = _cfg("pipeline_db", pipeline_db)
    out_dir = _cfg("output_dir")
    src_cfg = _cfg("sources_config", sources_config)

    if output:
        output_path = output
    else:
        today = date.today().isoformat()
        stem = f"validation_{report}_{today}" if report else f"validation_{today}"
        output_path = str(Path(out_dir) / f"{stem}.xlsx")

    conn = duckdb.connect(p_db)
    try:
        # Build Detail sheet — one entry per flagged report
        if report:
            report_names = [report]
        else:
            report_names = conn.execute(
                "SELECT DISTINCT report_name FROM validation_block ORDER BY report_name"
            ).df()["report_name"].tolist()

        if not report_names:
            click.echo("[error] No validation failures found.")
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
            # Resolve report table and pk_col
            table = rname
            pk_col = None
            if rep_config_obj:
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
                # Fall back to querying pk_value column directly from validation_block
                df = coerce_for_display(conn.execute(f"""
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

        # Build Summary sheet — count failures per report + check
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
        """.format("AND report_name = ?" if report else ""),
            [report] if report else []
        ).df())

        if detail_df.empty:
            scope = f" for report '{report}'" if report else ""
            click.echo(f"[error] No validation failures found{scope}.")
            return

        # Write two-sheet Excel
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        # Strip TIMESTAMPTZ for Excel compat
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
        click.echo("\n  Review the Detail sheet to identify which records need correction.")
        click.echo("  Fix them at source, re-ingest, and re-validate.")

    except ValueError as e:
        click.echo(f"[error] {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp export log
# ---------------------------------------------------------------------------


@export.command("log")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--output", default=None, help="Override output path (.csv).")
@click.option(
    "--severity",
    default=None,
    type=click.Choice(["info", "warn", "error"]),
    help="Export only events of this severity.",
)
@click.option(
    "--since",
    default=None,
    help="Export events on or after this date (YYYY-MM-DD).",
)
@click.option(
    "--clear",
    is_flag=True,
    default=False,
    help="Delete exported rows from pipeline_events after writing.",
)
def export_log(pipeline_db, output, severity, since, clear):
    """Export pipeline_events to CSV for archiving or review.

    Exports all events by default. Use --severity and --since to narrow
    the export. Use --clear to remove the exported rows from the table
    after writing — only the rows that matched the filters are deleted.

    \b
    Examples:
      vp export log
      vp export log --severity error
      vp export log --since 2026-01-01
      vp export log --severity warn --since 2026-03-01 --clear
    """
    import duckdb
    from pathlib import Path

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    log_dir = config_path_or_override("log_dir")

    if output:
        output_path = output
    else:
        today = date.today().isoformat()
        output_path = str(Path(log_dir) / f"pipeline_events_{today}.csv")

    conn = duckdb.connect(p_db)
    try:
        try:
            df = query_pipeline_events(conn, severity, since, order_desc=False)
        except ValueError:
            click.echo(f"[error] --since must be in YYYY-MM-DD format, got: {since}")
            return

        if df.empty:
            click.echo("[info] No events matched the given filters — nothing exported.")
            return

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        # Strip timezone for CSV compat
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].dt.tz_localize(None)

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

    except Exception as e:
        click.echo(f"[error] {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def export_commands(cli):
    cli.add_command(export)
