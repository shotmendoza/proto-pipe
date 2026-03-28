"""Setup commands — init, db-init, config."""
import shutil
from pathlib import Path

import click

from src.cli.helpers import config_path_or_override, config_settings
from src.io.settings import DEFAULT_SETTINGS_PATH, set_path, VALID_PATH_KEYS
from src.reports.validation_flags import init_validation_flags_table

_TEMPLATES_DIR = Path(__file__).parent.parent / "config"


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------
@click.command()
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
    """Scaffold config files into your project directory.

    Creates pipeline.yaml and the config/ folder with starter versions of
    sources_config.yaml, reports_config.yaml, deliverables_config.yaml,
    and views_config.yaml.

    \b
    Example:
      vp init
      vp init --output my_config --force
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
    click.echo("  1. Edit pipeline.yaml to set your paths")
    click.echo(f"  2. Edit {out}/sources_config.yaml to match your file naming conventions")
    click.echo(f"  3. Edit {out}/reports_config.yaml to define your reports and checks")
    click.echo(f"  4. Edit {out}/views_config.yaml to define shared transformation views")
    click.echo("  5. Run: vp db-init")


# ---------------------------------------------------------------------------
# db-init
# ---------------------------------------------------------------------------
@click.command("db-init")
@click.option("--pipeline-db",    default=None, help="Override pipeline DB path.")
@click.option("--watermark-db",   default=None, help="Override watermark DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--views-config",   default=None, help="Override views config path.")
def db_init(pipeline_db, watermark_db, sources_config, views_config):
    """Create DuckDB files and bootstrap tables from sources_config.yaml.

    Safe to re-run — existing tables are never overwritten. Also creates
    the flagged_rows and report_runs tables and registers any views defined
    in views_config.yaml.

    \b
    Example:
      vp db-init
    """
    import duckdb

    from src.io.ingest import init_db
    from src.io.registry import load_config
    from src.pipelines.watermark import WatermarkStore
    from src.reports.query import init_report_runs_table
    from src.reports.views import load_views_config, create_views

    src_cfg = config_path_or_override("sources_config", sources_config)
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    w_db = config_path_or_override("watermark_db", watermark_db)
    v_cfg = config_path_or_override("views_config", views_config)

    click.echo(f"\nInitialising pipeline DB: {p_db}")
    try:
        _config = load_config(src_cfg)
        init_db(p_db, _config["sources"])
    except FileNotFoundError:
        click.echo(f"[error] Could not find sources config at '{src_cfg}'")
        click.echo("Run `vp init` first.")
        return

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
    init_validation_flags_table(conn)
    init_report_runs_table(conn)
    click.echo("[ok] flagged_rows and report_runs tables ready")

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
    click.echo("[ok] Watermark table ready")
    click.echo("\nAll done. You're ready to run the pipeline.")


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------
@click.group()
def config():
    """View or update pipeline.yaml path settings."""
    pass


@config.command("show")
def config_show():
    """Print all current path settings from pipeline.yaml."""
    settings = config_settings()
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
    """Update a single path setting in pipeline.yaml.

    Sets a configuration value based on the provided key. This function allows users
    to assign a specific value to a configuration key, updating the application
    settings. If the key is invalid, an error message will be displayed, along with
    a list of valid keys.

    \b
    Example:
      vp config set incoming_dir /Volumes/SharedDrive/data/incoming/
    """
    try:
        set_path(key, value)
        click.echo(f"[ok] {key} = {value}")
    except ValueError as e:
        click.echo(f"[error] {e}")
        click.echo(f"Valid keys: {', '.join(VALID_PATH_KEYS)}")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
def setup_commands(cli: click.Group):
    """Adds commands to the specified CLI tool (Click).

    This function registers various command functions to a CLI tool instance, enabling
    user interaction with functionalities such as initialization, database configuration,
    and general configuration.

    :param cli: An instance of the CLI tool to which commands will be added.
    :type cli: Click Group or equivalent
    :return: None
    """
    cli.add_command(init)
    cli.add_command(db_init)
    cli.add_command(config)
