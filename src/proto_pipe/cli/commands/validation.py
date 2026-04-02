"""Validation commands — validate, checks, export-validation."""

import click

from proto_pipe.checks.helpers import load_custom_checks
from proto_pipe.io.config import config_path_or_override, load_config


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

@click.command()
@click.option("--pipeline-db",    default=None, help="Override pipeline DB path.")
@click.option("--watermark-db",   default=None, help="Override watermark DB path.")
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--table",          default=None, help="Run checks for one table only.")
@click.option(
    "--full",
    is_flag=True,
    default=False,
    help="Revalidate all records, ignoring validation_pass state.",
)
def validate(pipeline_db, watermark_db, reports_config, table, full):
    """Run registered checks and transforms against ingested tables.

    Incremental by default — only new or changed records are validated,
    tracked via validation_pass. Use --full to revalidate everything.

    Results and failures are written to validation_block in pipeline.db.
    Failures are warnings — they do not block deliverables.

    \b
    Examples:
      vp validate
      vp validate --table van
      vp validate --full
    """
    from proto_pipe.io.registry import register_from_config
    from proto_pipe.checks.registry import check_registry, report_registry
    from proto_pipe.pipelines.watermark import WatermarkStore
    from proto_pipe.reports.runner import run_all_reports
    import duckdb

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    w_db = config_path_or_override("watermark_db", watermark_db)
    rep_cfg = config_path_or_override("reports_config", reports_config)

    load_custom_checks(check_registry)
    _config = load_config(rep_cfg)
    register_from_config(_config, check_registry, report_registry)
    watermark_store = WatermarkStore(w_db)

    if table:
        reports = [
            r for r in report_registry.all()
            if r.get("source", {}).get("table") == table
        ]
        if not reports:
            click.echo(f"[warn] No reports registered for table '{table}'")
            return
    else:
        reports = report_registry.all()

    click.echo(f"\nRunning validation across {len(reports)} report(s)...")
    results = run_all_reports(
        report_registry,
        check_registry,
        watermark_store,
        pipeline_db=p_db,
        full_revalidation=full,
    )

    if table:
        results = [
            r for r in results if r["report"] in {rpt["name"] for rpt in reports}
        ]

    for r in results:
        status = r["status"]
        click.echo(f"\n  {r['report']} [{status}]")
        if status == "completed":
            for check_name, outcome in r["results"].items():
                mark = "✓" if outcome["status"] == "passed" else "✗"
                failed_count = outcome.get("failed_count", 0)
                label = check_name
                click.echo(f"  {mark} {label}")
                if outcome["status"] == "failed" and failed_count:
                    click.echo(f"       {failed_count} row(s) failed → validation_block")
                elif outcome["status"] == "error":
                    click.echo(f"       {outcome.get('error', '')}")
        elif status == "skipped":
            click.echo("       No pending records.")

    # Summarize validation_block after this run
    conn = duckdb.connect(p_db)
    try:
        total_failures = conn.execute(
            "SELECT count(*) FROM validation_block"
        ).fetchone()[0]

        if total_failures > 0:
            report_count = conn.execute(
                "SELECT count(DISTINCT report_name) FROM validation_block"
            ).fetchone()[0]
            click.echo(
                f"\n⚠  {total_failures} validation failure(s) across "
                f"{report_count} report(s)."
            )
            click.echo("\nTo review and fix:")
            click.echo("  vp validated               — browse failures by report")
            click.echo("  vp validated open <report> — export for external editing")
            click.echo("  vp validated edit --report <report> — edit inline")
        else:
            click.echo("\n✓  No validation failures.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# checks
# ---------------------------------------------------------------------------

@click.command()
def checks():
    """List all available built-in checks and their parameters.

    \b
    Example:
      vp checks
    """
    from proto_pipe.checks.built_in import BUILT_IN_CHECKS

    descriptions = {
        "null_check":      ("No params required",           "Checks all columns for null values"),
        "range_check":     ("col, min_val, max_val",        "Checks a column's values fall within a range"),
        "schema_check":    ("expected_cols (list)",         "Checks the table has the expected columns"),
        "duplicate_check": ("subset (list, optional)",      "Checks for duplicate rows"),
    }

    click.echo("\nBuilt-in checks:\n")
    for name in BUILT_IN_CHECKS:
        params, desc = descriptions.get(name, ("", ""))
        click.echo(f"  {name}")
        click.echo(f"    params: {params}")
        click.echo(f"    {desc}\n")


# ---------------------------------------------------------------------------
# export-validation
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
# Registration
# ---------------------------------------------------------------------------

def validation_commands(cli):
    cli.add_command(validate)
    cli.add_command(checks)
    cli.add_command(export_validation)
