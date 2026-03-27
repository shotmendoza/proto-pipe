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
import duckdb

from src.io.ingest import check_null_overwrites
from src.io.registry import load_config
from src.io.settings import (
    load_settings,
    set_path,
    VALID_PATH_KEYS,
    DEFAULT_SETTINGS_PATH,
)
from src.pipelines.corrections import dated_export_path
from src.registry.base import CheckRegistry
from src.reports.views import load_views_config, create_views, refresh_views

_TEMPLATES_DIR = Path(__file__).parent / "config"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _settings():
    """A utility function to load and return configurations.

    This function serves as a wrapper for loading application
    settings or configurations using the load_settings method.
    It operates internally and is not intended to be accessed
    outside the module.

    :return: Loaded application settings or configurations.
    """
    return load_settings()


def _p(
        key: str,
        override: str | None = None
) -> str:
    """Retrieve a specific path configuration value based on the provided key.

    This function fetches a value from a predefined settings dictionary using
    a given key. An optional override value can be provided, which will take
    precedence over the value corresponding to the key in the settings dictionary.

    :param key: The key used to identify the path in the settings dictionary.
    :param override: An optional value that overrides the value fetched from
        the settings dictionary. Defaults to None.
    :return: The path configuration value, either from the settings dictionary
        or the override if provided.
    """
    return override or _settings()["paths"][key]


def _load_custom_checks(check_registry: CheckRegistry) -> None:
    """Loads custom checks from a specified module path, if configured. This function
    retrieves the module path from settings and uses it to load custom checks into
    the provided registry. If no custom module path is specified, this function
    performs no operation.

    :param check_registry: The registry where the custom checks will be loaded.
    :type check_registry: Any
    :return: None
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
    """Initializes a configuration directory and files with defaults for a validation pipeline.

    Provides a way to set up the necessary configuration files for a validation pipeline, if they do
    not already exist in the specified directory. Will optionally overwrite existing files if the
    '--force' parameter is provided. Offers information on the next steps required for setup.

    :param output: Directory where configuration files will be generated.
    :param force: If set to True, any existing files in the output directory will be overwritten.
    :return: None

    :raises FileExistsError: When the configuration files already exist and '--force' is not used.
    """
    out = Path(output)
    out.mkdir(parents=True, exist_ok=True)

    files = {
        _TEMPLATES_DIR / "reports_config.yaml": out / "reports_config.yaml",
        _TEMPLATES_DIR / "sources_config.yaml": out / "sources_config.yaml",
        _TEMPLATES_DIR / "deliverables_config.yaml": out / "deliverables_config.yaml",
        _TEMPLATES_DIR / "pipeline.yaml": Path("../pipeline.yaml"),
        _TEMPLATES_DIR / "views_config.yaml": out / "views_config.yaml",
    }

    for src, dest in files.items():
        if dest.exists() and not force:
            click.echo(f"  [skip] {dest} already exists (use --force to overwrite)")
            continue
        shutil.copy(src, dest)
        click.echo(f"  [ok]   {dest}")

    click.echo("\nNext steps:")
    click.echo(f"  1. Edit pipeline.yaml to set your paths")
    click.echo(f"  2. Edit {out}/sources_config.yaml to match your file naming conventions")
    click.echo(f"  3. Edit {out}/reports_config.yaml to define your reports and checks")
    click.echo(f"  4. Edit {out}/views_config.yaml to define shared transformation views")
    click.echo(f"  5. Run: vp db-init")


# ---------------------------------------------------------------------------
# db-init
# ---------------------------------------------------------------------------


@cli.command("db-init")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--watermark-db", default=None, help="Override watermark DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--views-config",   default=None, help="Override views config path.")  # NEW
def db_init(pipeline_db, watermark_db, sources_config, views_config):
    """
    Initializes the pipeline database and watermark database for the data pipeline. This command
    specifically prepares the necessary tables to store flagged rows, report runs, and watermark
    data. Additionally, it updates settings based on the provided configurations for sources and
    views.

    :param pipeline_db: Optional override for the path to the pipeline database. If not provided,
        a default path will be used.
    :type pipeline_db: str, optional

    :param watermark_db: Optional override for the path to the watermark database. If not provided,
        a default path will be used.
    :type watermark_db: str, optional

    :param sources_config: Optional override for the sources configuration file path. The file
        contains sources details required for database initialization.
    :type sources_config: str, optional

    :param views_config: Optional override for the views configuration file path. This can specify
        additional configurations for view-related setup.
    :type views_config: str, optional

    :return: None
    """
    from src.io.ingest import init_db
    from src.io.registry import load_config
    from src.pipelines.watermark import WatermarkStore
    from src.reports.query import init_report_runs_table

    import duckdb

    src_cfg = _p("sources_config", sources_config)
    p_db = _p("pipeline_db", pipeline_db)
    w_db = _p("watermark_db", watermark_db)
    v_cfg = _p("views_config", views_config)

    click.echo(f"\nInitialising pipeline DB: {p_db}")
    try:
        _config = load_config(src_cfg)
        init_db(p_db, _config["sources"])
    except FileNotFoundError:
        click.echo(f"[error] Could not find sources config at '{src_cfg}'")
        click.echo(f"Run `validation-pipeline init` first.")
        return

    # Also ensure flagged_rows and report_runs tables exist
    conn = duckdb.connect(p_db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS flagged_rows (
            id           VARCHAR PRIMARY KEY,
            table_name   VARCHAR NOT NULL,
            check_name   VARCHAR,
            reason       VARCHAR,
            flagged_at   TIMESTAMPTZ NOT NULL
        )
    """)
    init_report_runs_table(conn)
    click.echo(f"[ok] flagged_rows and report_runs tables ready")

    # NEW: create views (skips existing ones)
    views = load_views_config(v_cfg)
    if views:
        click.echo(f"\nCreating views from: {v_cfg}")
        try:
            create_views(conn, views, replace=False, skip_missing_tables=True)
        except Exception as e:
            click.echo(f"[error] Could not create views: {e}")
    else:
        click.echo(f"[skip] No views defined in {v_cfg}")

    conn.close()

    click.echo(f"\nInitialising watermark DB: {w_db}")
    Path(w_db).parent.mkdir(parents=True, exist_ok=True)
    WatermarkStore(w_db)
    click.echo(f"[ok] Watermark table ready")
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
        click.echo(f"{key:<25} {val}")


@config.command("set")
@click.argument("key")
@click.argument("value")
def config_set(
        key: str,
        value: str
):
    """Sets a configuration value based on the provided key. This function allows users
    to assign a specific value to a configuration key, updating the application
    settings. If the key is invalid, an error message will be displayed, along with
    a list of valid keys.

    :param key: The configuration key to be set. Must be a valid key from the valid
        path keys.
    :param value: The value to assign to the specified configuration key.
    :return: None

    :raises ValueError: If the provided key is not valid, an error is raised.
    """
    try:
        set_path(key, value)
        click.echo(f"[ok] {key} = {value}")
    except ValueError as e:
        click.echo(f"[error] {e}")
        click.echo(f"Valid keys: {', '.join(VALID_PATH_KEYS)}")


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


@cli.command()
@click.option(
    "--incoming-dir",
    default=None,
    help="Override incoming files directory."
)
@click.option(
    "--pipeline-db",
    default=None,
    help="Override pipeline DB path."
)
@click.option(
    "--sources-config",
    default=None,
    help="Override sources config path."
)
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
    skipped = sum(1 for v in summary.values() if v["status"] == "skipped")
    failed = sum(1 for v in summary.values() if v["status"] == "failed")
    flagged = sum(v.get("flagged", 0) for v in summary.values())

    click.echo(
        f"\n  {ok} loaded, {skipped} skipped, {failed} failed, "
        f"{flagged} row conflict(s) flagged — see ingest_log for details."
    )


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


@cli.command()
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--watermark-db", default=None, help="Override watermark DB path.")
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--table", default=None, help="Run checks for one table only.")
def validate(
        pipeline_db,
        watermark_db,
        reports_config,
        table
):
    """Validates the pipeline and watermark databases by executing the registered
    checks and generating reports. This function allows optional filtering
    on a specific table.

    :param pipeline_db: Overrides the default pipeline database path if provided
    :param watermark_db: Overrides the default watermark database path if provided
    :param reports_config: Overrides the default reports configuration file path if provided
    :param table: Runs checks and generates reports for a specific table, if specified
    :return: None
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
    """Updates the data table in the pipeline database based on the provided file and configuration.
    This function processes updates to a specific database table by ingesting data from a file.
    It supports two modes: appending to the existing table or replacing the table's contents
    entirely. The function also handles schema migration for the database table if necessary.

    Parameters:
    ----------
    :param table: Name of the target database table to update
    :type table: str
    :param filepath: Path to the file containing data to ingest
    :type filepath: str
    :param pipeline_db: (Optional) Custom path to the DuckDB pipeline database. If not
        provided, the default location is used
    :type pipeline_db: str or None
    :param sources_config: (Optional) Path to the YAML file defining source configurations.
        If not provided, the default configuration path is used
    :type sources_config: str or None
    :param mode: Operation mode for updating the table. Choices are "append" to add new rows
        and "replace" to recreate the table. The default mode is "append"
    :type mode: str

    Returns:
    -------
    :return: None
    """
    from src.io.ingest import (
        load_file,
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
        df = load_file(path)
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


def run_all_pull_report_step(
    conn,  # single open DuckDB connection passed in
    deliverable,
    del_config,
    rep_config,
    out_dir,
    p_db,
    query_table,
    produce_deliverable,
    v_cfg,
):
    """
    Extracted pull-report logic for run-all that reuses the existing conn
    instead of opening a new one, and refreshes views before producing output.
    This function is illustrative — integrate the logic inline in your run-all.
    """
    from src.reports.views import load_views_config, refresh_views

    deliverables = {d["name"]: d for d in del_config.get("deliverables", [])}

    if deliverable not in deliverables:
        click.echo(f"  [error] No deliverable named '{deliverable}'")
        return

    # Refresh views before producing deliverables
    click.echo("\n── Refresh Views ───────────────────────────")
    views = load_views_config(v_cfg)
    if views:
        try:
            refresh_views(conn, views)
        except Exception as e:
            click.echo(f"  [error] Could not refresh views: {e}")
            return
    else:
        click.echo("  [skip] No views defined")

    click.echo("\n── Pull Report ─────────────────────────────")
    d = deliverables[deliverable]

    # Load reports config once
    report_defs = {r["name"]: r for r in rep_config.get("reports", [])}

    report_dataframes = {}
    for report_cfg in d["reports"]:
        rname = report_cfg["name"]
        sql_file = report_cfg.get("sql_file")

        if sql_file:
            try:
                df = query_table(conn, sql_file=sql_file)
                report_dataframes[rname] = df
                click.echo(f"  [query] {rname} → {len(df)} rows (sql_file)")
            except Exception as e:
                click.echo(f"  [error] sql_file failed for '{rname}': {e}")
        else:
            if rname not in report_defs:
                click.echo(f"  [warn] '{rname}' not in reports_config.yaml, skipping")
                continue
            table = report_defs[rname]["source"]["table"]
            df = query_table(conn, table, filters=report_cfg.get("filters", {}))
            report_dataframes[rname] = df

    produce_deliverable(d, report_dataframes, out_dir, p_db)


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
def run_all(
        conn,
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

    p_db = _p("pipeline_db", pipeline_db)
    w_db = _p("watermark_db", watermark_db)
    inc_dir = _p("incoming_dir", incoming_dir)
    src_cfg = _p("sources_config")
    rep_cfg = _p("reports_config")
    del_cfg = _p("deliverables_config")
    v_cfg = _p("views_config")
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

    if flagged_count > 0 and not ignore_flagged:
        conn.close()
        click.echo(f"\n⚠ {flagged_count} flagged row(s) require manual review before producing deliverables.")
        click.echo("Use: vp export-flagged --table <name>  to export them for correction.")
        click.echo("Query the flagged_rows table in your pipeline DB to review.")
        click.echo("Re-run with --ignore-flagged to produce the deliverable anyway.")
        return

    # Step 3 — Refresh views (NEW)
    click.echo("\n── Refresh Views ───────────────────────────")
    views = load_views_config(v_cfg)
    if views:
        try:
            refresh_views(conn, views)
        except Exception as e:
            click.echo(f"  [error] Could not refresh views: {e}")
            conn.close()
            return
    else:
        click.echo("  [skip] No views defined")

    # Step 4 — Pull report
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
                rdefs = {r["name"]: r for r in rep_config.get("reports", [])}
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
        click.echo(f"{name}")
        click.echo(f"params: {params}")
        click.echo(f"{desc}\n")


# ---------------------------------------------------------------------------
# export-flagged — NEW command
# ---------------------------------------------------------------------------
@cli.command("export-flagged")
@click.option("--table", required=True, help="Table to export flagged rows from.")
@click.option("--output", default=None,  help="Output CSV path. Defaults to flagged_<table>.csv in output_dir.")
@click.option("--pipeline-db", default=None,  help="Override pipeline DB path.")
def export_flagged(table, output, pipeline_db):
    """Export flagged rows for a table to CSV for manual correction.

    The file includes all source columns plus _flag_reason and _flag_id.
    Fix the values in the file, then run import-corrections to apply them.

    \b
    Example:
      vp export-flagged --table sales
      vp export-flagged --table sales --output /tmp/sales_fixes.csv
    """
    from src.pipelines.corrections import export_flagged as _export
    import duckdb

    p_db = _p("pipeline_db", pipeline_db)
    out_dir = _p("output_dir")
    output_path = output or dated_export_path(out_dir, table)

    conn = duckdb.connect(p_db)
    try:
        count = _export(conn, table, output_path)
        click.echo(f"[ok] {count} flagged row(s) exported to: {output_path}")
        click.echo(f"\nFix the values, then run:")
        click.echo(f"vp import-corrections {output_path} --table {table}")
    except ValueError as e:
        click.echo(f"[error] {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# import-corrections — NEW command
# ---------------------------------------------------------------------------

@cli.command("import-corrections")
@click.argument("filepath")
@click.option("--table",          required=True, help="Table to apply corrections to.")
@click.option("--key",            default=None,  help="Override primary key column (default: from sources_config.yaml).")
@click.option("--pipeline-db",    default=None,  help="Override pipeline DB path.")
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
    from src.pipelines.corrections import import_corrections as _import
    from src.io.registry import load_config
    import duckdb

    p_db = _p("pipeline_db", pipeline_db)
    src_cfg = _p("sources_config", sources_config)
    path = Path(filepath)

    if not path.exists():
        click.echo(f"[error] File not found: {filepath}")
        return

    # Resolve primary key: --key overrides sources_config.yaml
    primary_key = key
    if not primary_key:
        _config = load_config(src_cfg)
        sources = {s["target_table"]: s for s in config["sources"]}
        if table not in sources:
            click.echo(f"[error] No source defined for '{table}' in sources_config.yaml")
            click.echo(f"Use --key to specify the primary key directly.")
            return
        primary_key = sources[table].get("primary_key")
        if not primary_key:
            click.echo(f"[error] No primary_key defined for '{table}' in sources_config.yaml")
            click.echo(f"Add primary_key to the source or use --key.")
            return

    conn = duckdb.connect(p_db)
    try:
        result = _import(conn, table, str(path), primary_key)
        click.echo(f"[ok] {result['updated']} row(s) updated in '{table}'")
        click.echo(f"[ok] {result['flagged_cleared']} flag(s) cleared from flagged_rows")
    except (ValueError, FileNotFoundError) as e:
        click.echo(f"[error] {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# refresh-views — NEW command
# ---------------------------------------------------------------------------
@cli.command("refresh-views")
@click.option("--pipeline-db",  default=None, help="Override pipeline DB path.")
@click.option("--views-config", default=None, help="Override views config path.")
def refresh_views_cmd(pipeline_db, views_config):
    """
    Drop and recreate all views from views_config.yaml.

    Run this after editing a view SQL file. Views are also refreshed
    automatically during run-all.

    \b
    Example:
      vp refresh-views
    """
    from src.reports.views import load_views_config, refresh_views
    import duckdb

    p_db = _p("pipeline_db",  pipeline_db)
    v_cfg = _p("views_config", views_config)

    views = load_views_config(v_cfg)
    if not views:
        click.echo(f"  [skip] No views defined in {v_cfg}")
        return

    click.echo(f"\nRefreshing {len(views)} view(s) from: {v_cfg}")
    conn = duckdb.connect(p_db)
    try:
        refresh_views(conn, views)
        click.echo(f"\n  Done.")
    except Exception as e:
        click.echo(f"  [error] {e}")
    finally:
        conn.close()


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

    p_db = _p("pipeline_db",    pipeline_db)
    src_cfg = _p("sources_config", sources_config)

    config = load_config(src_cfg)
    sources = {s["target_table"]: s for s in config["sources"]}

    if table not in sources:
        click.echo(f"  [error] No source defined for '{table}' in sources_config.yaml")
        return

    primary_key = sources[table].get("primary_key")
    if not primary_key:
        click.echo(
            f"[error] No primary_key defined for '{table}' in sources_config.yaml\n"
            f"Add primary_key to the source definition to enable conflict detection."
        )
        return

    conn = duckdb.connect(p_db)
    try:
        flagged = check_null_overwrites(conn, table, primary_key)
        if flagged:
            click.echo(f"  [ok] {flagged} new conflict(s) flagged in '{table}'")
            click.echo(f"       Run: vp export-flagged --table {table}")
        else:
            click.echo(f"  [ok] No new conflicts found in '{table}'")
    finally:
        conn.close()


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

    p_db = _p("pipeline_db", pipeline_db)
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
        click.echo(f"\n  {total} flagged row(s) across {df['table_name'].nunique()} table(s):\n")

        current_table = None
        for _, row in df.iterrows():
            if row["table_name"] != current_table:
                current_table = row["table_name"]
                click.echo(f"  {current_table}")
            click.echo(
                f"    {row['flagged_count']:>4}  [{row['check_name']}]  {row['reason']}"
            )

        click.echo(f"\n  Run: vp flagged-list --table <name>   to see affected rows")
        click.echo(f"  Run: vp export-flagged --table <name>  to export for correction")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# NEW: flagged-list
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

    Examples:
      vp flagged-list --table sales
      vp flagged-list --table sales --check duplicate_conflict
      vp flagged-list --table sales --limit 20
    """
    import duckdb

    p_db = _p("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        # Pull flag metadata
        query = """
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

        merged = flags.head(limit).merge(
            source_df,
            left_on="row_index",
            right_on="_row_index",
            how="left",
        ).drop(columns=["_row_index", "row_index", "_flag_id"])

        for _, row in merged.iterrows():
            click.echo(f"  ── [{row['check_name']}] {row['_flag_reason']}")
            click.echo(f"     flagged: {row['flagged_at']}")
            data_cols = [
                c for c in merged.columns
                if c not in ("check_name", "_flag_reason", "flagged_at")
            ]
            for col in data_cols:
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
# NEW: flagged-clear
# ---------------------------------------------------------------------------

@click.command("flagged-clear")
@click.option("--table", required=True, help="Table to clear flags for.")
@click.option("--check", default=None, help="Only clear flags for this check name.")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def flagged_clear(table, check, yes, pipeline_db):
    """Clear all flagged rows for a table without applying corrections.

    Use this when you've reviewed the flags and decided to proceed
    without making corrections (e.g. the data is acceptable as-is).
    This action cannot be undone.

    Examples:
      vp flagged-clear --table sales
      vp flagged-clear --table sales --check duplicate_conflict
      vp flagged-clear --table sales --yes (skip confirmation)
    """
    import duckdb

    p_db = _p("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        query = "SELECT count(*) FROM flagged_rows WHERE table_name = ?"
        params = [table]
        if check:
            query += " AND check_name = ?"
            params.append(check)

        count = conn.execute(query, params).fetchone()[0]

        if count == 0:
            click.echo(f"  No flagged rows to clear for '{table}'.")
            return

        scope = f"check '{check}'" if check else "all checks"
        if not yes:
            click.confirm(
                f"  Clear {count} flagged row(s) for '{table}' ({scope})? "
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


@click.command("check-null-overwrites")
@click.option("--table", required=True)
@click.option("--pipeline-db", default=None)
@click.option("--sources-config", default=None)
def check_null_overwrites_cmd(table, pipeline_db, sources_config):
    """Manually scan a table for rows with the same primary key but different
    content, and flag any new conflicts not already in flagged_rows.

    \b
    Example:
      vp check-null-overwrites --table sales
    """
    from src.io.ingest import check_null_overwrites
    from src.io.registry import load_config
    import duckdb

    p_db = _p("pipeline_db", pipeline_db)
    src_cfg = _p("sources_config", sources_config)
    _config = load_config(src_cfg)
    sources = {s["target_table"]: s for s in _config["sources"]}

    if table not in sources:
        click.echo(f"  [error] No source defined for '{table}' in sources_config.yaml")
        return

    primary_key = sources[table].get("primary_key")
    if not primary_key:
        click.echo(
            f"  [error] No primary_key defined for '{table}' in sources_config.yaml"
        )
        return

    conn = duckdb.connect(p_db)
    try:
        flagged = check_null_overwrites(conn, table, primary_key)
        if flagged:
            click.echo(f"  [ok] {flagged} new conflict(s) flagged in '{table}'")
            click.echo(f"       Run: vp export-flagged --table {table}")
        else:
            click.echo(f"  [ok] No new conflicts found in '{table}'")
    finally:
        conn.close()


@click.command("flagged-summary")
@click.option("--pipeline-db", default=None)
def flagged_summary(pipeline_db):
    """
    Print a count of open flagged rows by table and reason.

    \b
    Example:
      vp flagged-summary
    """
    import duckdb

    p_db = _p("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        df = conn.execute("""
                          SELECT table_name, check_name, reason, count(*) AS flagged_count,
                                 min(flagged_at) AS first_flagged, max(flagged_at) AS last_flagged
                          FROM flagged_rows
                          GROUP BY table_name, check_name, reason
                          ORDER BY table_name, flagged_count DESC
                          """).df()

        if df.empty:
            click.echo("\n  No flagged rows — all clear.")
            return

        total = df["flagged_count"].sum()
        click.echo(
            f"\n  {total} flagged row(s) across {df['table_name'].nunique()} table(s):\n"
        )

        current_table = None
        for _, row in df.iterrows():
            if row["table_name"] != current_table:
                current_table = row["table_name"]
                click.echo(f"  {current_table}")
            click.echo(
                f"    {row['flagged_count']:>4}  [{row['check_name']}]  {row['reason']}"
            )

        click.echo(f"\n  Run: vp flagged-list --table <n>   to see affected rows")
        click.echo(f"  Run: vp export-flagged --table <n>  to export for correction")
    finally:
        conn.close()


@click.command("flagged-list")
@click.option("--table", required=True)
@click.option("--check", default=None)
@click.option("--limit", default=50, show_default=True)
@click.option("--pipeline-db", default=None)
def flagged_list(table, check, limit, pipeline_db):
    """
    Print flagged rows for a table inline in the terminal.

    \b
    Examples:
      vp flagged-list --table sales
      vp flagged-list --table sales --check duplicate_conflict
      vp flagged-list --table sales --limit 20
    """
    import duckdb

    p_db = _p("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        query = """
                 SELECT id AS _flag_id, row_index, check_name, reason AS _flag_reason, flagged_at
                 FROM flagged_rows WHERE table_name = ? \
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

        source_df = conn.execute(f'SELECT * FROM "{table}"').df()
        source_df["_row_index"] = source_df.index

        merged = (
            flags.head(limit)
            .merge(
                source_df,
                left_on="row_index",
                right_on="_row_index",
                how="left",
            )
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
