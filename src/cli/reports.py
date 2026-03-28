"""Report commands — pull-report, run-all, refresh-views."""

import click

from .helpers import config_path_or_override, load_custom_checks


# ---------------------------------------------------------------------------
# pull-report
# ---------------------------------------------------------------------------
@click.command("pull-report")
@click.argument("deliverable_name")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--deliverables-config", default=None, help="Override deliverables config path.")
@click.option("--output-dir", default=None, help="Override output directory.")
@click.option("--date-from", default=None, help="Override date filter from (YYYY-MM-DD).")
@click.option("--date-to", default=None, help="Override date filter to (YYYY-MM-DD).")
@click.option("--date-col", default=None, help="Column to apply --date-from/--date-to on.")
def pull_report(
    deliverable_name,
    pipeline_db,
    deliverables_config,
    output_dir,
    date_from,
    date_to,
    date_col,
):
    """Query tables and write deliverable output (CSV or Excel).

    Config filters are applied by default. Use --date-from, --date-to,
    and --date-col to override the date range at runtime. CLI date flags
    only apply to filter-based reports — sql_file reports ignore them.

    \b
    Example:
      vp pull-report monthly_sales_pack
      vp pull-report monthly_sales_pack --date-from 2026-01-01 --date-to 2026-03-31 --date-col order_date
    """
    import duckdb

    from src.io.registry import load_config
    from src.reports.runner import run_deliverable
    from src.reports.query import query_table

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    del_cfg = config_path_or_override("deliverables_config", deliverables_config)
    out_dir = config_path_or_override("output_dir", output_dir)

    _config = load_config(del_cfg)
    deliverables = {d["name"]: d for d in _config.get("deliverables", [])}

    if deliverable_name not in deliverables:
        click.echo(f"[error] No deliverable named '{deliverable_name}'")
        click.echo(f"Available: {', '.join(deliverables.keys())}")
        return

    deliverable = deliverables[deliverable_name]
    conn = duckdb.connect(p_db)

    # Build CLI date override if provided
    cli_date_filter = None
    if date_col and (date_from or date_to):
        entry = {"col": date_col}
        if date_from:
            entry["from"] = date_from
        if date_to:
            entry["to"] = date_to
        cli_date_filter = {"date_filters": [entry]}

    rep_config = load_config(config_path_or_override("reports_config"))
    report_defs = {r["name"]: r for r in rep_config.get("reports", [])}

    # Query each report's table
    report_dataframes = {}
    for report_cfg in deliverable["reports"]:
        report_name = report_cfg["name"]
        sql_file = report_cfg.get("sql_file")

        if sql_file:
            # sql_file path — execute SQL directly, skip filter system entirely
            try:
                df = query_table(conn, table=None, sql_file=sql_file)
                report_dataframes[report_name] = df
                click.echo(f"  [query] {report_name} → {len(df)} rows (sql_file)")
            except Exception as e:
                click.echo(f"  [error] Could not execute sql_file for '{report_name}': {e}")
        else:
            # Filter path — look up source table from reports_config
            # Find the source table for this report from reports_config
            if report_name not in report_defs:
                click.echo(f"  [warn] Report '{report_name}' not found in reports_config.yaml, skipping")
                continue

            table = report_defs[report_name]["source"]["table"]
            filters = report_cfg.get("filters", {})

            try:
                df = query_table(conn, table, filters=filters, cli_overrides=cli_date_filter)
                report_dataframes[report_name] = df
                click.echo(f"[query] {report_name} → {len(df)} rows")
            except Exception as e:
                click.echo(f"[error] Could not query '{table}': {e}")

    conn.close()

    if not report_dataframes:
        click.echo("[error] No data to write.")
        return

    click.echo(f"\nWriting deliverable: {deliverable_name}")
    run_deliverable(deliverable, report_dataframes, out_dir, p_db)


# ---------------------------------------------------------------------------
# run-all
# ---------------------------------------------------------------------------
@click.command("run-all")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--watermark-db", default=None, help="Override watermark DB path.")
@click.option("--incoming-dir", default=None, help="Override incoming files directory.")
@click.option(
    "--deliverable", default=None, help="Pull a specific deliverable after validation."
)
@click.option(
    "--ignore-flagged",
    is_flag=True,
    default=False,
    help="Produce deliverable even if flagged rows exist.",
)
def run_all(
        pipeline_db,
        watermark_db,
        incoming_dir,
        deliverable,
        ignore_flagged
):
    """Chain ingest → validate → pull-report in one command.

    Stops before pull-report if flagged rows exist, unless --ignore-flagged is set.
    Auto-fixed rows are applied during validate. Complex cases are written to
    the flagged_rows table for manual review.

    \b
    Example:
      vp run-all --deliverable monthly_pack
      vp run-all --deliverable monthly_pack --ignore-flagged
    """
    from src.pipelines.watermark import WatermarkStore
    from src.registry.base import check_registry, report_registry
    from src.io.registry import load_config, register_from_config
    from src.reports.runner import run_all_reports
    from src.reports.runner import run_deliverable
    from src.reports.query import query_table
    from src.io.ingest import ingest_directory
    from src.reports.views import refresh_views, load_views_config

    import duckdb

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    w_db = config_path_or_override("watermark_db", watermark_db)
    inc_dir = config_path_or_override("incoming_dir", incoming_dir)
    src_cfg = config_path_or_override("sources_config")
    rep_cfg = config_path_or_override("reports_config")
    del_cfg = config_path_or_override("deliverables_config")
    v_cfg = config_path_or_override("views_config")
    out_dir = config_path_or_override("output_dir")

    # Step 1 — Ingest
    click.echo("\n── Ingest ──────────────────────────────────")
    sources_config = load_config(src_cfg)
    ingest_directory(inc_dir, sources_config["sources"], p_db)

    # Step 2 — Validate
    click.echo("\n── Validate ────────────────────────────────")
    rep_config = load_config(rep_cfg)
    load_custom_checks(check_registry)
    register_from_config(rep_config, check_registry, report_registry)
    watermark_store = WatermarkStore(w_db)
    # Pass pipeline_db so the runner writes per-row validation flags
    run_all_reports(report_registry, check_registry, watermark_store, pipeline_db=p_db)

    # Check for flagged rows before producing deliverable
    conn = duckdb.connect(p_db)
    flagged_count = conn.execute("SELECT count(*) FROM flagged_rows").fetchone()[0]

    # TODO: This can be split into its own function, just like 3B
    # Step 3a — Ingest conflicts (flagged_rows) — hard block
    from src.reports.validation_flags import count_validation_flags
    ingest_conflict_count = conn.execute(
        "SELECT count(*) FROM flagged_rows"
    ).fetchone()[0]

    if ingest_conflict_count > 0 and not ignore_flagged:
        conn.close()
        click.echo(
            f"\n⚠  {ingest_conflict_count} ingest conflict(s) require review before producing deliverables."
        )
        click.echo(
            "These are rows that arrived with conflicting values for existing records."
        )
        click.echo(
            "Run: vp flagged-summary — to see a breakdown by table"
        )
        click.echo("Run: vp export-flagged --table <n> — to export for correction")
        click.echo("Re-run with --ignore-flagged to produce the deliverable anyway.")
        return

    # Step 3b — Validation flags — warn only, deliverable still produced
    val_flag_count = count_validation_flags(conn)
    if val_flag_count > 0:
        click.echo(
            f"\n⚠  {val_flag_count} validation flag(s) found. Deliverable will still be produced."
        )
        click.echo("Run: vp export-validation to review flagged records.")

    # Step 4 — Refresh views
    click.echo("\n── Refresh Views ───────────────────────────")
    views = load_views_config(v_cfg)
    if views:
        try:
            refresh_views(conn, views)
        except Exception as e:
            click.echo(f"[error] Could not refresh views: {e}")
            conn.close()
            return
    else:
        click.echo("[skip] No views defined")

    # Step 5 — Pull report
    if deliverable:
        click.echo("\n── Pull Report ─────────────────────────────")
        del_config = load_config(del_cfg)
        deliverables = {d["name"]: d for d in del_config.get("deliverables", [])}

        if deliverable not in deliverables:
            click.echo(f"[error] No deliverable named '{deliverable}'")
            conn.close()
            return

        d = deliverables[deliverable]
        report_dataframes = {}
        rdefs = {r["name"]: r for r in rep_config.get("reports", [])}

        for report_cfg in d["reports"]:
            rname = report_cfg["name"]
            sql_file = report_cfg.get("sql_file")

            if sql_file:
                try:
                    df = query_table(conn, sql_file=sql_file)
                    report_dataframes[rname] = df
                    click.echo(f"[query] {rname} → {len(df)} rows (sql_file)")
                except Exception as e:
                    click.echo(f"[error] sql_file failed for '{rname}': {e}")
            else:
                if rname not in rdefs:
                    continue
                table = rdefs[rname]["source"]["table"]
                df = query_table(conn, table, filters=report_cfg.get("filters", {}))
                report_dataframes[rname] = df

        conn.close()
        run_deliverable(d, report_dataframes, out_dir, p_db)
    else:
        conn.close()
        click.echo("\n  No --deliverable specified. Ingest and validate complete.")


# ---------------------------------------------------------------------------
# refresh-views
# ---------------------------------------------------------------------------


@click.command("refresh-views")
@click.option("--pipeline-db",  default=None, help="Override pipeline DB path.")
@click.option("--views-config", default=None, help="Override views config path.")
def refresh_views_cmd(pipeline_db, views_config):
    """Drop and recreate all views from views_config.yaml.

    Run this after editing a view SQL file. Views are also refreshed
    automatically during run-all before deliverables are produced.

    \b
    Example:
      vp refresh-views
    """
    import duckdb

    from src.reports.views import load_views_config, refresh_views

    p_db = config_path_or_override("pipeline_db",  pipeline_db)
    v_cfg = config_path_or_override("views_config", views_config)

    views = load_views_config(v_cfg)
    if not views:
        click.echo(f"[skip] No views defined in {v_cfg}")
        return

    click.echo(f"\nRefreshing {len(views)} view(s) from: {v_cfg}")
    conn = duckdb.connect(p_db)
    try:
        refresh_views(conn, views)
        click.echo("\nDone.")
    except Exception as e:
        click.echo(f"[error] {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
def reports_commands(cli):
    cli.add_command(pull_report)
    cli.add_command(run_all)
    cli.add_command(refresh_views_cmd)
