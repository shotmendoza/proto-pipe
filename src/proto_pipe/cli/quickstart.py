"""Setup commands — init, db-init, config."""
import shutil
from importlib.resources import files
from pathlib import Path

import click

from proto_pipe.io.config import config_settings, config_path_or_override
from proto_pipe.io.settings import DEFAULT_SETTINGS_PATH, set_path, VALID_PATH_KEYS
from proto_pipe.reports.validation_flags import init_validation_flags_table

_TEMPLATES_DIR = files("proto_pipe") / "templates"


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

    # [Templates] Copying the config templates so that the user can modify them
    _files = {
        _TEMPLATES_DIR / "pipeline.yaml": Path("pipeline.yaml"),
        _TEMPLATES_DIR / "reports_config.yaml": out / "reports_config.yaml",
        _TEMPLATES_DIR / "sources_config.yaml": out / "sources_config.yaml",
        _TEMPLATES_DIR / "deliverables_config.yaml": out / "deliverables_config.yaml",
        _TEMPLATES_DIR / "views_config.yaml": out / "views_config.yaml",
    }

    for src, dest in _files.items():
        if dest.exists() and not force:
            click.echo(f"[skip] {dest} already exists (use --force to overwrite)")
            continue
        shutil.copy(src, dest)
        click.echo(f"[ok] {dest}")

    # [Next Steps] Let the user know what to do next
    click.echo("\nNext steps:")
    click.echo("1. Edit `pipeline.yaml` to confirm your paths")
    click.echo("2. Run: `vp db-init`")
    click.echo("Additionally, you can run `vp docs` to read through the docs.")


# ---------------------------------------------------------------------------
# docs page
# ---------------------------------------------------------------------------
@click.command()
def docs():
    """Open the proto-pipe documentation in your browser."""
    import webbrowser
    url = "https://shotmendoza.github.io/proto-pipe"
    webbrowser.open(url)
    click.echo(f"Opening docs: {url}")


# ---------------------------------------------------------------------------
# db-init
# ---------------------------------------------------------------------------
@click.command("db-init")
@click.option("--pipeline-db",  default=None, help="Override pipeline DB path.")
@click.option("--watermark-db", default=None, help="Override watermark DB path.")
@click.option("--migrate", is_flag=True, default=False, help="Apply schema migrations to existing tables.")
def db_init(pipeline_db, watermark_db, migrate):
    """Create DuckDB files and bootstrap tables from sources_config.yaml.

    Safe to re-run — existing tables are never dropped. Pass --migrate
    to apply any pending schema changes to existing tables.

    \b
    Example:
      vp db-init
      vp db-init --migrate
    """

    import duckdb

    from proto_pipe.io.ingest import init_db
    from proto_pipe.pipelines.watermark import WatermarkStore
    from proto_pipe.reports.query import init_report_runs_table

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    w_db = config_path_or_override("watermark_db", watermark_db)

    click.echo(f"\nInitializing pipeline DB: {p_db}")
    init_db(p_db)

    # [Create Infrastructure Tables] Create the report run and flagged tables
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
    conn.execute("""
    CREATE TABLE IF NOT EXISTS check_params_history (
        id          VARCHAR PRIMARY KEY,
        check_name  VARCHAR NOT NULL,
        report_name VARCHAR NOT NULL,
        table_name  VARCHAR NOT NULL,
        param_name  VARCHAR NOT NULL,
        param_value VARCHAR,
        recorded_at TIMESTAMPTZ NOT NULL
    )
""")
    init_validation_flags_table(conn)
    init_report_runs_table(conn)

    if migrate:
        from proto_pipe.reports.validation_flags import migrate_validation_flags

        migrate_validation_flags(conn)
        click.echo("[ok] Schema migrations applied")

    click.echo("[ok] Infrastructure tables ready (Report Runs + Flagged Tables)")
    conn.close()

    # [Create Watermark Table] Create the watermark table for most recents
    click.echo(f"\nInitializing watermark DB: {w_db}")
    Path(w_db).parent.mkdir(parents=True, exist_ok=True)
    WatermarkStore(w_db)
    click.echo("[ok] Watermark table ready")

    click.echo("\nNext steps:")
    click.echo("1. Run: vp new-source (define your data sources)")
    click.echo("2. Run: vp ingest (load files into the pipeline)")


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
    cli.add_command(docs)
