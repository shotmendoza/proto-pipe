"""CLI entrypoint.

Commands:
    init            — scaffold config files into your project
    db-init         — create DuckDB files and bootstrap tables
    config show     — print current path settings
    config set      — update a path in pipeline.yaml
    ingest          — load files from incoming dir into DuckDB
    validate        — run checks against ingested tables
    update-table    — re-ingest a specific table from a specific file
    pull-report     — query a table with filters, write deliverable output
    run-all         — chain ingest → validate → pull-report (stops on flagged rows)
    checks          — list all available built-in checks
"""

import shutil
from pathlib import Path

import click

from src.io.settings import (
    load_settings,
    set_path,
    VALID_PATH_KEYS,
    DEFAULT_SETTINGS_PATH,
)

_TEMPLATES_DIR = Path(__file__).parent / "config"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _settings():
    return load_settings()


def _p(key: str, override: str | None = None) -> str:
    """Return a path from settings, with an optional CLI override."""
    return override or _settings()["paths"][key]


def _load_custom_checks(check_registry) -> None:
    """
    If custom_checks_module is set in pipeline.yaml, import it and register
    any @custom_check decorated functions. Called before any command that
    touches the check registry so custom checks are always available.
    """
    from src.io.registry import load_custom_checks_module

    module_path = _settings().get("custom_checks_module")
    if module_path:
        load_custom_checks_module(module_path, check_registry)


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


@click.group()
def cli():
    """Validation pipeline — manage your config and pipeline."""
    pass


@cli.command()
@click.option(
    "--output",
    default="config",
    show_default=True,
    help="Directory to write config files into.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Overwrite existing config files if present.",
)
def init(output: str, force: bool):
    """
    Scaffold starter config files and pipeline.yaml into your project.

    Run db-init afterwards to create the DuckDB databases.
    """
    out = Path(output)
    out.mkdir(parents=True, exist_ok=True)

    files = {
        _TEMPLATES_DIR / "reports_config.yaml": out / "reports_config.yaml",
        _TEMPLATES_DIR / "sources_config.yaml": out / "sources_config.yaml",
        _TEMPLATES_DIR / "deliverables_config.yaml": out / "deliverables_config.yaml",
        _TEMPLATES_DIR / "pipeline.yaml": Path("../pipeline.yaml"),
    }

    for src, dest in files.items():
        if dest.exists() and not force:
            click.echo(f"  [skip] {dest} already exists (use --force to overwrite)")
            continue
        shutil.copy(src, dest)
        click.echo(f"  [ok]   {dest}")

    click.echo("\nNext steps:")
    click.echo(f"  1. Edit pipeline.yaml to set your paths")
    click.echo(
        f"  2. Edit {out}/sources_config.yaml to match your file naming conventions"
    )
    click.echo(f"  3. Edit {out}/reports_config.yaml to define your reports and checks")
    click.echo(f"  4. Run: validation-pipeline db-init")


# ---------------------------------------------------------------------------
# db-init
# ---------------------------------------------------------------------------


@cli.command("db-init")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--watermark-db", default=None, help="Override watermark DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
def db_init(pipeline_db, watermark_db, sources_config):
    """
    Create DuckDB files and bootstrap tables from sources_config.yaml.
    Safe to re-run — existing tables and watermarks are left untouched.
    """
    from src.io.ingest import init_db
    from src.io.registry import load_config
    from src.pipelines.watermark import WatermarkStore
    from src.reports.query import init_report_runs_table

    import duckdb

    src_cfg = _p("sources_config", sources_config)
    p_db = _p("pipeline_db", pipeline_db)
    w_db = _p("watermark_db", watermark_db)

    click.echo(f"\nInitialising pipeline DB: {p_db}")
    try:
        _config = load_config(src_cfg)
        init_db(p_db, _config["sources"])
    except FileNotFoundError:
        click.echo(f"  [error] Could not find sources config at '{src_cfg}'")
        click.echo(f"          Run `validation-pipeline init` first.")
        return

    # Also ensure flagged_rows and report_runs tables exist
    conn = duckdb.connect(p_db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS flagged_rows (
            id           VARCHAR PRIMARY KEY,
            table_name   VARCHAR NOT NULL,
            row_index    INTEGER,
            check_name   VARCHAR,
            reason       VARCHAR,
            flagged_at   TIMESTAMPTZ NOT NULL
        )
    """)
    init_report_runs_table(conn)
    conn.close()
    click.echo(f"  [ok]   flagged_rows and report_runs tables ready")

    click.echo(f"\nInitialising watermark DB: {w_db}")
    Path(w_db).parent.mkdir(parents=True, exist_ok=True)
    WatermarkStore(w_db)
    click.echo(f"  [ok]   Watermark table ready")
    click.echo(f"\nAll done. You're ready to run the pipeline.")


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------


@cli.group()
def config():
    """View or update pipeline.yaml path settings."""
    pass


@config.command("show")
def config_show():
    """Print all current path settings from pipeline.yaml."""
    settings = _settings()
    click.echo(f"\nSettings from: {DEFAULT_SETTINGS_PATH}\n")
    for key, val in settings["paths"].items():
        click.echo(f"  {key:<25} {val}")


@config.command("set")
@click.argument("key")
@click.argument("value")
def config_set(key: str, value: str):
    """
    Update a path setting in pipeline.yaml.

    \b
    Keys:
      sources_config, reports_config, deliverables_config,
      pipeline_db, watermark_db, incoming_dir, output_dir
    \b
    Example:
      validation-pipeline config set incoming_dir /data/drops/
    """
    try:
        set_path(key, value)
        click.echo(f"  [ok] {key} = {value}")
    except ValueError as e:
        click.echo(f"  [error] {e}")
        click.echo(f"  Valid keys: {', '.join(VALID_PATH_KEYS)}")


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


@cli.command()
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
    """
    Scan the incoming directory and load matching files into DuckDB.

    Failures are logged to ingest_log and skipped — the run continues.
    New tables are created lazily if they weren't present at db-init.
    New columns are auto-migrated without touching existing rows.
    """
    from src.io.registry import load_config, register_from_config
    from src.io.ingest import ingest_directory
    from src.registry.base import check_registry, report_registry

    inc_dir = _p("incoming_dir", incoming_dir)
    p_db = _p("pipeline_db", pipeline_db)
    src_cfg = _p("sources_config", sources_config)

    _config = load_config(src_cfg)

    cr, rr = (check_registry, report_registry) if validate else (None, None)
    if validate:
        _load_custom_checks(check_registry)
        rep_cfg = load_config(_p("reports_config"))
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
    failed = sum(1 for v in summary.values() if v["status"] == "failed")
    click.echo(f"\n  {ok} loaded, {failed} failed — see ingest_log table for details.")


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


@cli.command()
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--watermark-db", default=None, help="Override watermark DB path.")
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--table", default=None, help="Run checks for one table only.")
def validate(pipeline_db, watermark_db, reports_config, table):
    """Run registered checks against ingested tables.

    Uses watermarks to check only new/modified rows since the last run.
    Failures are logged — the run continues across all reports.
    """
    from src.io.registry import load_config, register_from_config
    from src.registry.base import check_registry, report_registry
    from src.pipelines.watermark import WatermarkStore
    from src.reports.runner import run_all_reports

    p_db = _p("pipeline_db", pipeline_db)
    w_db = _p("watermark_db", watermark_db)
    rep_cfg = _p("reports_config", reports_config)

    _load_custom_checks(check_registry)
    _config = load_config(rep_cfg)
    register_from_config(_config, check_registry, report_registry)
    watermark_store = WatermarkStore(w_db)

    # Filter to one table if requested
    if table:
        reports = [
            r
            for r in report_registry.all()
            if r.get("source", {}).get("table") == table
        ]
        if not reports:
            click.echo(f"  [warn] No reports registered for table '{table}'")
            return
    else:
        reports = report_registry.all()

    click.echo(f"\nRunning validation across {len(reports)} report(s)...")
    results = run_all_reports(report_registry, check_registry, watermark_store)

    for r in results:
        status = r["status"]
        click.echo(f"\n  {r['report']} [{status}]")
        if status == "completed":
            for check_name, outcome in r["results"].items():
                mark = "✓" if outcome["status"] == "passed" else "✗"
                click.echo(f"    {mark} {check_name}")
                if outcome["status"] == "failed":
                    click.echo(f"      {outcome['error']}")


# ---------------------------------------------------------------------------
# update-table
# ---------------------------------------------------------------------------


@cli.command("update-table")
@click.argument("table")
@click.argument("filepath")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option(
    "--mode",
    default="append",
    show_default=True,
    type=click.Choice(["append", "replace"]),
    help="append: add rows. replace: rebuild table from this file.",
)
def update_table(table, filepath, pipeline_db, sources_config, mode):
    """
    Re-ingest a specific file into a specific table.

    \b
    Example:
      validation-pipeline update-table sales data/drops/sales_march.csv
      validation-pipeline update-table sales data/drops/sales_march.csv --mode replace
    """
    from src.io.ingest import (
        _load_file,
        _init_ingest_log,
        _table_exists,
        _auto_migrate,
        _log_ingest,
    )
    from src.io.registry import load_config
    import duckdb

    p_db = _p("pipeline_db", pipeline_db)
    src_cfg = _p("sources_config", sources_config)
    path = Path(filepath)

    if not path.exists():
        click.echo(f"  [error] File not found: {filepath}")
        return

    config = load_config(src_cfg)
    sources = {s["target_table"]: s for s in config["sources"]}

    if table not in sources:
        click.echo(
            f"  [error] No source defined for table '{table}' in sources_config.yaml"
        )
        return

    source = sources[table]

    try:
        df = _load_file(path)
    except Exception as e:
        click.echo(f"  [error] Could not load '{filepath}': {e}")
        return

    conn = duckdb.connect(p_db)
    _init_ingest_log(conn)

    if mode == "replace" or not _table_exists(conn, table):
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM df')
        new_cols = []
        click.echo(f"  [ok] '{table}' replaced from '{path.name}' ({len(df)} rows)")
    else:
        new_cols = _auto_migrate(conn, table, df)
        conn.execute(f'INSERT INTO "{table}" SELECT * FROM df')
        click.echo(
            f"  [ok] '{table}' updated from '{path.name}' ({len(df)} rows appended)"
        )

    _log_ingest(conn, path.name, table, "ok", rows=len(df), new_cols=new_cols)
    conn.close()


# ---------------------------------------------------------------------------
# pull-report
# ---------------------------------------------------------------------------


@cli.command("pull-report")
@click.argument("deliverable_name")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option(
    "--deliverables-config", default=None, help="Override deliverables config path."
)
@click.option("--output-dir", default=None, help="Override output directory.")
@click.option(
    "--date-from", default=None, help="Override date filter from (YYYY-MM-DD)."
)
@click.option("--date-to", default=None, help="Override date filter to (YYYY-MM-DD).")
@click.option(
    "--date-col", default=None, help="Column to apply --date-from/--date-to on."
)
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
    and --date-col to override the date range at runtime.

    \b
    Example:
      validation-pipeline pull-report monthly_sales_pack
      validation-pipeline pull-report monthly_sales_pack --date-from 2026-01-01 --date-to 2026-03-31 --date-col order_date
    """
    from src.io.registry import load_config
    from src.reports.runner import run_deliverable
    from src.reports.query import query_table

    import duckdb

    p_db = _p("pipeline_db", pipeline_db)
    del_cfg = _p("deliverables_config", deliverables_config)
    out_dir = _p("output_dir", output_dir)

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

    # Query each report's table
    report_dataframes = {}
    for report_cfg in deliverable["reports"]:
        report_name = report_cfg["name"]

        # Find the source table for this report from reports_config
        rep_config = load_config(_p("reports_config"))
        report_defs = {r["name"]: r for r in rep_config.get("reports", [])}

        if report_name not in report_defs:
            click.echo(
                f"[warn] Report '{report_name}' not found in reports_config.yaml, skipping"
            )
            continue

        table = report_defs[report_name]["source"]["table"]
        filters = report_cfg.get("filters", {})

        try:
            df = query_table(
                conn, table, filters=filters, cli_overrides=cli_date_filter
            )
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


@cli.command("run-all")
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
def run_all(pipeline_db, watermark_db, incoming_dir, deliverable, ignore_flagged):
    """
    Chain ingest → validate → pull-report in one command.

    Stops before pull-report if flagged rows exist, unless --ignore-flagged is set.
    Auto-fixed rows are applied during validate. Complex cases are written to
    the flagged_rows table for manual review.
    """
    from src.pipelines.watermark import WatermarkStore
    from src.registry.base import check_registry, report_registry
    from src.io.registry import load_config, register_from_config
    from src.reports.runner import run_all_reports
    from src.reports.runner import run_deliverable
    from src.reports.query import query_table
    from src.io.ingest import ingest_directory

    import duckdb

    p_db = _p("pipeline_db", pipeline_db)
    w_db = _p("watermark_db", watermark_db)
    inc_dir = _p("incoming_dir", incoming_dir)
    src_cfg = _p("sources_config")
    rep_cfg = _p("reports_config")
    del_cfg = _p("deliverables_config")
    out_dir = _p("output_dir")

    # Step 1 — Ingest
    click.echo("\n── Ingest ──────────────────────────────────")
    sources_config = load_config(src_cfg)
    ingest_directory(inc_dir, sources_config["sources"], p_db)

    # Step 2 — Validate
    click.echo("\n── Validate ────────────────────────────────")
    rep_config = load_config(rep_cfg)
    _load_custom_checks(check_registry)
    register_from_config(rep_config, check_registry, report_registry)
    watermark_store = WatermarkStore(w_db)
    run_all_reports(report_registry, check_registry, watermark_store)

    # Check for flagged rows before producing deliverable
    conn = duckdb.connect(p_db)
    flagged_count = conn.execute("SELECT count(*) FROM flagged_rows").fetchone()[0]
    conn.close()

    if flagged_count > 0 and not ignore_flagged:
        click.echo(
            f"\n⚠ {flagged_count} flagged row(s) require manual review before producing deliverables."
        )
        click.echo("  Query the flagged_rows table in your pipeline DB to review.")
        click.echo("  Re-run with --ignore-flagged to produce the deliverable anyway.")
        return

    # Step 3 — Pull report
    if deliverable:
        click.echo("\n── Pull Report ─────────────────────────────")
        del_config = load_config(del_cfg)
        deliverables = {d["name"]: d for d in del_config.get("deliverables", [])}

        if deliverable not in deliverables:
            click.echo(f"[error] No deliverable named '{deliverable}'")
            return

        d = deliverables[deliverable]
        conn = duckdb.connect(p_db)
        report_dataframes = {}

        for report_cfg in d["reports"]:
            rname = report_cfg["name"]
            rdefs = {r["name"]: r for r in rep_config.get("reports", [])}
            if rname not in rdefs:
                continue
            table = rdefs[rname]["source"]["table"]
            df = query_table(conn, table, filters=report_cfg.get("filters", {}))
            report_dataframes[rname] = df

        conn.close()
        run_deliverable(d, report_dataframes, out_dir, p_db)
    else:
        click.echo("\n  No --deliverable specified. Ingest and validate complete.")


# ---------------------------------------------------------------------------
# checks
# ---------------------------------------------------------------------------


@cli.command()
def checks():
    """List all built-in checks and their parameters."""
    from src.checks.built_in import BUILT_IN_CHECKS

    click.echo("\nBuilt-in checks:\n")
    descriptions = {
        "null_check": ("No params required", "Checks all columns for null values"),
        "range_check": (
            "col, min_val, max_val",
            "Checks a column's values fall within a range",
        ),
        "schema_check": (
            "expected_cols (list)",
            "Checks the table has the expected columns",
        ),
        "duplicate_check": ("subset (list, optional)", "Checks for duplicate rows"),
    }
    for name in BUILT_IN_CHECKS:
        params, desc = descriptions.get(name, ("", ""))
        click.echo(f"  {name}")
        click.echo(f"    params: {params}")
        click.echo(f"    {desc}\n")
