"""Setup commands — init (config + db), config, docs."""
import shutil
from importlib.resources import files
from pathlib import Path

import click

from proto_pipe.io.config import config_settings, config_path_or_override, set_path
from proto_pipe.constants import VALID_PATH_KEYS, DEFAULT_SETTINGS_PATH
_TEMPLATES_DIR = files("proto_pipe") / "templates"


# ---------------------------------------------------------------------------
# vp init  (group — bare runs both config + db)
# ---------------------------------------------------------------------------
@click.group(invoke_without_command=True)
@click.pass_context
def init(ctx):
    """Scaffold config files and bootstrap the database.

    Running bare `vp init` executes both steps: config scaffold then DB
    bootstrap. Use subcommands to run either step individually.

    \b
    Examples:
      vp init              # scaffold configs + bootstrap DB
      vp init config       # scaffold config files only
      vp init db           # bootstrap database only
    """
    if ctx.invoked_subcommand is None:
        # Bare `vp init` — run both steps sequentially
        ctx.invoke(init_config)
        click.echo("")  # visual separator
        ctx.invoke(init_db)


# ---------------------------------------------------------------------------
# vp init config
# ---------------------------------------------------------------------------
@init.command("config")
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
def init_config(output: str = "config", force: bool = False):
    """Scaffold config files into your project directory.

    Creates pipeline.yaml and the config/ folder with starter versions of
    sources_config.yaml, reports_config.yaml, deliverables_config.yaml,
    and views_config.yaml.

    \b
    Example:
      vp init config
      vp init config --output my_config --force
    """
    out = Path(output)
    out.mkdir(parents=True, exist_ok=True)

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

    click.echo("\nNext steps:")
    click.echo("1. Edit `pipeline.yaml` to confirm your paths")
    click.echo("2. Run: `vp init db`")
    click.echo("Additionally, you can run `vp docs` to read through the docs.")


# ---------------------------------------------------------------------------
# vp init db
# ---------------------------------------------------------------------------
@init.command("db")
@click.option("--pipeline-db",  default=None, help="Override pipeline DB path.")
@click.option("--watermark-db", default=None, help="Override watermark DB path.")
@click.option("--migrate", is_flag=True, default=False, help="Apply schema migrations to existing tables.")
def init_db(pipeline_db=None, watermark_db=None, migrate=False):
    """Create DuckDB files and bootstrap pipeline tables.

    Safe to re-run — all table creation uses CREATE IF NOT EXISTS.
    Pass --migrate to rename/update tables from an older schema version.

    Bootstraps: ingest_state, source_pass, source_block, validation_pass,
    validation_block, check_registry_metadata, column_type_registry.

    \b
    Example:
      vp init db
      vp init db --migrate
    """
    import duckdb

    from proto_pipe.io.ingest import init_db as _init_db
    from proto_pipe.pipelines.watermark import WatermarkStore

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    w_db = config_path_or_override("watermark_db", watermark_db)

    click.echo(f"\nInitializing pipeline DB: {p_db}")

    _init_db(p_db)
    click.echo("[ok] Pipeline tables ready")

    if migrate:
        from proto_pipe.io.migration import migrate_pipeline_schema
        conn = duckdb.connect(p_db)
        try:
            applied = migrate_pipeline_schema(conn)
            if applied:
                click.echo("\n[ok] Schema migrations applied:")
                for m in applied:
                    click.echo(f"  • {m}")
            else:
                click.echo("[ok] No schema migrations needed — already up to date")
        finally:
            conn.close()

    click.echo(f"\nInitializing watermark DB: {w_db}")
    Path(w_db).parent.mkdir(parents=True, exist_ok=True)
    WatermarkStore(w_db)
    click.echo("[ok] Watermark table ready")

    click.echo("\nNext steps:")
    click.echo("1. Run: vp new source (define your data sources)")
    click.echo("2. Run: vp ingest (load files into the pipeline)")


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
    """Register setup commands onto the CLI root."""
    cli.add_command(init)
    cli.add_command(config)
    cli.add_command(docs)
