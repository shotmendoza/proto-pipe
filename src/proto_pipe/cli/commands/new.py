"""vp new — create new pipeline resources.

Wraps the existing scaffold commands under a unified group.
Command logic lives in scaffold.py until Session B moves it here.
"""
from pathlib import Path

import click
import duckdb
import questionary

from proto_pipe.cli.scaffold import (
    _scan_incoming,
    get_original_func,
    build_rich_sql_scaffold,
    _scan_macros,
    filter_unconfigured,
    _group_files_by_pattern,
)
from proto_pipe.io.db import get_all_source_tables
from proto_pipe.io.config import load_config, write_config, load_settings, config_path_or_override
from proto_pipe.cli.prompts import SourceConfigPrompter
from proto_pipe.constants import DEFAULT_SETTINGS_PATH


@click.group("new", context_settings={"max_content_width": 120})
def new_cmd():
    """Create new pipeline resources.

    \b
    Examples:
      vp new source
      vp new report
      vp new deliverable
      vp new view
      vp new macro
      vp new sql
    """
    pass


def new_commands(cli: click.Group) -> None:
    cli.add_command(new_cmd)


@click.command("new-source")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--incoming-dir", default=None, help="Override incoming directory path.")
def new_source(sources_config, incoming_dir):
    """Interactively define a new data source and add it to sources_config.yaml.

    \b
    Example:
      vp new source
    """
    from proto_pipe.io.config import SourceConfig
    from proto_pipe.io.db import get_registry_hints, write_registry_types

    src_cfg = config_path_or_override("sources_config", sources_config)
    inc_dir = config_path_or_override("incoming_dir", incoming_dir)
    settings = load_settings()
    pipeline_db = settings["paths"]["pipeline_db"]

    config = SourceConfig(src_cfg)

    click.echo("\n── New Source ──────────────────────────────")

    all_files = _scan_incoming(inc_dir)
    files = filter_unconfigured(all_files, config.all())

    if not files:
        if not all_files:
            click.echo(
                f"\n[error] No files found in '{inc_dir}'."
                f"\n  Add a matching file to '{inc_dir}' first, then run: vp new source"
            )
        else:
            click.echo(
                f"\n[error] All {len(all_files)} file(s) in '{inc_dir}' are already"
                f" covered by an existing source.\n"
                f"  Run 'vp edit source' to update an existing source,"
                f" or add a new file first."
            )
        return

    file_groups = _group_files_by_pattern(files)
    selected_file, suggested = SourceConfigPrompter.prompt_file_group(file_groups)

    if suggested is None:
        click.echo("Cancelled.")
        return

    # Load ALL files matching the suggested pattern via DuckDB with
    # union_by_name=True so the combined schema covers every column
    # that may appear across the file group — not just one file.
    group_files = file_groups.get(suggested, [])
    sample = None

    if group_files:
        if len(group_files) > 1:
            click.echo(
                f"\n  {len(group_files)} files match '{suggested}'"
                f" — reading all to derive the combined schema.\n"
            )
        try:
            file_paths = [str(Path(inc_dir) / f) for f in group_files]
            with duckdb.connect(pipeline_db) as _conn:
                path_list = ", ".join(f"'{p}'" for p in file_paths)
                sample = _conn.execute(
                    f"SELECT * FROM read_csv([{path_list}],"
                    f" union_by_name=true) LIMIT 1000"
                ).df()
                from proto_pipe.io.db import coerce_for_display

                sample = coerce_for_display(sample)
        except Exception as e:
            click.echo(f"  [warn] Could not read files for schema preview: {e}")

    registry_hints: dict = {}
    if sample is not None:
        # Strip blank/whitespace-only column names — these are CSV artefacts
        # (trailing commas, empty header cells) and must never be registered.
        file_cols = [c for c in sample.columns if not c.startswith("_") and c.strip()]
        try:
            with duckdb.connect(pipeline_db) as conn:
                registry_hints = get_registry_hints(conn, file_cols)
        except Exception as e:
            print(f"[warn] Could not load column type hints from registry: {e}")
    # existing source name (e.g. to add a new pattern).
    existing_lookup = {s["name"]: s for s in config.all()}

    prompter = SourceConfigPrompter(
        sample_df=sample,
        registry_hints=registry_hints,
        existing_sources_lookup=existing_lookup,
    )

    if not prompter.run(config.names(), suggested):
        click.echo("Cancelled.")
        return

    if prompter.confirmed_types:
        try:
            with duckdb.connect(pipeline_db) as conn:
                write_registry_types(
                    conn, prompter.source["name"], prompter.confirmed_types
                )
        except Exception as e:
            click.echo(
                f"[warn] Column types were not saved to column_type_registry: {e}\n"
                f"  Has 'vp init db' been run? Types confirmed this session are lost.\n"
                f"  Run 'vp init db' then 'vp new source' again to re-confirm them."
            )
    config.add_or_update(prompter.source)
    click.echo(f"\n[ok] Source '{prompter.source['name']}' added to {src_cfg}")
    click.echo("\nNext steps:")
    click.echo("1. Review the entry in sources_config.yaml if needed")
    click.echo("2. Run: vp ingest")


@click.command("new-report")
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def new_report(reports_config, pipeline_db):
    """Interactively define a new report and add it to reports_config.yaml.

    \b
    Example:
      vp new report
    """
    from proto_pipe.checks.registry import CheckRegistry, CheckParamInspector
    from proto_pipe.checks.built_in import BUILT_IN_CHECKS
    from proto_pipe.io.config import config_path_or_override, ReportConfig
    from proto_pipe.checks.helpers import load_custom_checks_module
    from proto_pipe.io.config import load_settings
    from proto_pipe.io.db import init_check_registry_metadata
    from proto_pipe.cli.prompts import ReportConfigPrompter

    rep_cfg = config_path_or_override("reports_config", reports_config)
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    settings = load_settings()
    multi_select = settings.get("multi_select_params", True)

    config = ReportConfig(rep_cfg)

    check_registry = CheckRegistry()
    for name, func in BUILT_IN_CHECKS.items():
        check_registry.register(name, func)

    module_path = settings.get("custom_checks_module")
    if module_path:
        load_custom_checks_module(module_path, check_registry)

    if not check_registry.available():
        click.echo("\n[warn] No checks available. Add built-in or custom checks first.")
        return

    all_tables = get_all_source_tables(p_db, {"reports": config.all()})
    if not all_tables:
        click.echo(
            "\n  No tables found. Run: vp ingest to load files first."
        )
        return

    # all_tables is list[tuple[str, int]] — prompt_table in prompts.py
    # builds the annotated questionary.Choice objects (CLAUDE.md: prompts.py
    # owns all CLI formatting; command files never call questionary directly).
    table_choices = all_tables

    click.echo("\n── New Report ──────────────────────────────")
    click.echo("  Press ESC at any prompt to go back to the previous step.\n")

    prompter = ReportConfigPrompter(
        check_registry=check_registry,
        p_db=p_db,
        multi_select=multi_select,
    )

    conn = duckdb.connect(p_db)
    init_check_registry_metadata(conn)
    for check_name in check_registry.available():
        original = get_original_func(check_name, check_registry)
        if original is not None:
            CheckParamInspector(original).write_to_db(conn, check_name)

    try:
        if not prompter.run(table_choices, config.names(), conn):
            click.echo("Cancelled.")
            return
    finally:
        conn.close()

    report = {
        "name": prompter.name,
        "source": {"type": "duckdb", "path": p_db, "table": prompter.table},
        "options": {"parallel": False},
    }
    if prompter.alias_map:
        report["alias_map"] = prompter.alias_map
    report["checks"] = prompter.check_entries

    config.add_or_update(report)
    click.echo(f"\n[ok] Report '{prompter.name}' added to {rep_cfg}")
    click.echo("\nNext steps:")
    click.echo("1. Review the entry in reports_config.yaml if needed")
    click.echo("2. Run: vp validate")


@click.command("new-deliverable")
@click.option("--deliverables-config", default=None)
@click.option("--reports-config", default=None)
@click.option("--sources-config", default=None)
@click.option("--sql-dir", default=None)
def new_deliverable(deliverables_config, reports_config, sources_config, sql_dir):
    """Interactively define a new deliverable and add it to deliverables_config.yaml.

    \b
    Example:
      vp new-deliverable
    """
    from proto_pipe.io.config import config_path_or_override, DeliverableConfig
    from proto_pipe.io.config import load_config, load_settings
    from proto_pipe.cli.prompts import DeliverableConfigPrompter

    del_cfg = config_path_or_override("deliverables_config", deliverables_config)
    rep_cfg = config_path_or_override("reports_config", reports_config)
    src_cfg = config_path_or_override("sources_config", sources_config)
    settings = load_settings()
    sql_directory = sql_dir or settings["paths"].get("sql_dir", "sql")

    config = DeliverableConfig(del_cfg)
    rep_config = load_config(rep_cfg)
    src_config = load_config(src_cfg)

    available_reports = [r["name"] for r in rep_config.get("reports", [])]
    if not available_reports:
        click.echo("\n  No reports configured yet. Run: vp new report")
        return

    click.echo("\n── New Deliverable ─────────────────────────")

    prompter = DeliverableConfigPrompter(
        rep_config=rep_config,
        src_config=src_config,
        sql_dir=sql_directory,
    )

    if not prompter.run(config.names(), available_reports):
        click.echo("Cancelled.")
        return

    config.add_or_update(prompter.deliverable)

    click.echo(
        f"\n[ok] Deliverable '{prompter.deliverable['name']}' added to {del_cfg}"
    )
    click.echo("\nNext steps:")
    sql_file = prompter.deliverable.get("sql_file")
    if sql_file:
        click.echo(f"1. Edit {sql_file} with your transformation query")
        click.echo(
            f"2. Run: vp deliver {prompter.deliverable['name']}"
        )
    else:
        click.echo(
            f"1. Run: vp deliver {prompter.deliverable['name']}"
        )


@click.command("new-view")
@click.option("--name", required=True, help="Name for the new deliverable (e.g. carrier_a_report).")
@click.option("--output-dir", default=None, help="Override SQL output directory.")
def new_view(name: str, output_dir: str):
    """Scaffold a starter SQL file for a new deliverable view and sql transformations.

    \b
    Example:
      vp new-deliverable --name carrier_a_report
    """
    from proto_pipe.io.config import load_settings

    settings = load_settings()
    sql_dir = Path(output_dir or settings["paths"].get("sql_dir", "sql"))
    sql_dir.mkdir(parents=True, exist_ok=True)

    dest = sql_dir / f"{name}.sql"
    if dest.exists():
        click.echo(f"  [skip] {dest} already exists (delete it first to regenerate)")
        return

    template = f"""\
-- {name}.sql
-- Deliverable query for: {name}
--
-- This query defines what gets written to the output file.
-- Use DuckDB SQL syntax. Reference ingested tables directly by name.
-- Date filtering should be written inline here — no placeholders needed.
--
-- Example:
--   SELECT order_id, customer_id, price, region
--   FROM sales
--   WHERE updated_at >= '2024-01-01'
--     AND region = 'West'
--   ORDER BY updated_at DESC;

SELECT *
FROM <table>
WHERE <date_col> >= '<from_date>'
ORDER BY <date_col> DESC;
"""

    dest.write_text(template)
    click.echo(f"[ok] {dest}")
    click.echo(f"\nNext steps:")
    click.echo(f"1. Edit {dest} with your query")
    click.echo(f"2. Add an entry in deliverables_config.yaml referencing {name}.sql")
    click.echo(f"3. Run: vp deliver {name}")


@click.command("new-macro")
@click.argument("name")
@click.option("--macros-dir", default=None, help="Override macros directory.")
def new_macro(name: str, macros_dir: str | None):
    """Scaffold a new SQL macro file in the macros directory.

    Creates a templated .sql file with a CREATE OR REPLACE MACRO stub.
    Registers macros_dir in pipeline.yaml if not already set.

    \b
    Example:
      vp new-macro normalize_transaction_type
    """
    from proto_pipe.io.config import load_settings

    settings = load_settings()
    macro_dir_path = macros_dir or settings.get("macros_dir", "macros")

    dest_dir = Path(macro_dir_path)
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest = dest_dir / f"{name}.sql"
    if dest.exists():
        click.echo(f"  [skip] {dest} already exists — delete it first to regenerate")
        return

    template = f"""\
-- {name}.sql
-- Macro: describe what this macro does
--
-- Macros are registered at pipeline startup and available in all
-- view and deliverable SQL queries.
--
-- Use CREATE OR REPLACE so re-running vp init db is idempotent.

CREATE OR REPLACE MACRO {name}(val) AS
    CASE
        WHEN val = 'old_value' THEN 'new_value'
        ELSE val
    END;
"""
    dest.write_text(template)
    click.echo(f"[ok] {dest}")

    # Add macros_dir to pipeline.yaml if not already present
    pipeline_yaml = DEFAULT_SETTINGS_PATH
    if pipeline_yaml.exists():
        doc = load_config(pipeline_yaml)
        if "macros_dir" not in doc:
            doc["macros_dir"] = macro_dir_path
            write_config(doc, pipeline_yaml)
            click.echo(f"[ok] Added macros_dir = '{macro_dir_path}' to pipeline.yaml")

    click.echo(f"\nNext steps:")
    click.echo(f"1. Edit {dest} with your macro logic")
    click.echo(f"2. Run: vp init db   (re-registers all macros)")


@click.command("new-sql")
@click.argument("name", required=False)
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--sql-dir", default=None, help="Override SQL output directory.")
def new_sql(name, reports_config, sources_config, sql_dir):
    """Scaffold an annotated SQL file for a deliverable query.

    Creates a .sql file with join stubs, macro references, and inline comments
    so you can write your carrier queries faster. Does not modify any config
    file — wire it up in deliverables_config.yaml when ready.

    \\b
    Example:
      vp new-sql carrier_a_sales
      vp new-sql # interactive — prompts for name and tables
    """
    from proto_pipe.io.config import config_path_or_override, load_settings

    rep_cfg = config_path_or_override("reports_config", reports_config)
    src_cfg = config_path_or_override("sources_config", sources_config)
    settings = load_settings()

    sql_directory = sql_dir or settings["paths"].get("sql_dir", "sql")
    macros_dir = settings.get("macros_dir", "macros")

    rep_config = load_config(rep_cfg)
    src_config = load_config(src_cfg)

    available_reports = [r["name"] for r in rep_config.get("reports", [])]

    if not available_reports:
        click.echo("  No reports configured yet. Run: vp new report")
        return

    click.echo("\n── New SQL File ────────────────────────────")

    # Name
    if not name:
        name = questionary.text("SQL file name (e.g. carrier_a_sales):").ask()
        if not name:
            click.echo("Cancelled.")
            return

    dest = Path(sql_directory) / f"{name}.sql"
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists():
        overwrite = questionary.confirm(f"{dest} already exists. Overwrite?").ask()
        if not overwrite:
            click.echo("Cancelled.")
            return

    # Select reports / tables
    selected = questionary.checkbox(
        "Which reports should this query pull from?",
        choices=available_reports,
    ).ask()
    if not selected:
        click.echo("Cancelled.")
        return

    scaffold = build_rich_sql_scaffold(
        deliverable_name=name,
        selected_reports=selected,
        reports_config=rep_config,
        sources_config=src_config,
        macros_dir=macros_dir,
    )

    dest.write_text(scaffold)
    click.echo(f"\n[ok] {dest}")

    # Show which macros were found
    macros = _scan_macros(macros_dir)
    if macros:
        click.echo(f"\nMacros available in this query:")
        for sig in macros:
            click.echo(f"  {sig}")

    click.echo(f"\nNext steps:")
    click.echo(f"1. Edit {dest} with your query logic")
    click.echo(f"2. Reference it in deliverables_config.yaml:\n" f'sql_file: "{dest}"')


#####################
# REGISTRATION
####################

def _register(cli_group: click.Group) -> None:
    """Register all vp new subcommands onto the group."""

    cli_group.add_command(new_source, name="source")
    cli_group.add_command(new_report, name="report")
    cli_group.add_command(new_deliverable, name="deliverable")
    cli_group.add_command(new_view, name="view")
    cli_group.add_command(new_macro, name="macro")
    cli_group.add_command(new_sql, name="sql")


_register(new_cmd)
