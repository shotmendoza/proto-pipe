"""vp edit — edit existing pipeline resources."""
from datetime import timezone, datetime

import click
import duckdb
import questionary

from proto_pipe.io.config import config_path_or_override, load_config, load_settings
from proto_pipe.constants import PIPELINE_TABLES
from proto_pipe.io.db import get_all_tables


@click.group("edit", context_settings={"max_content_width": 120})
def edit_cmd():
    """Edit existing pipeline resources.

    \b
    Examples:
      vp edit source
      vp edit report
      vp edit deliverable
      vp edit table
    """
    pass


@edit_cmd.command("source")
@click.option("--table", default=None, help="Source table name to edit.")
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Flag incompatible rows and proceed with migration instead of blocking.",
)
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
def edit_source(table, force, pipeline_db, sources_config):
    """Edit an existing source configuration.

    If column types change and the table is already ingested, runs a
    pre-scan to check compatibility. Blocks on conflicts unless --force
    is passed, which flags incompatible rows and proceeds.

    \b
    Examples:
      vp edit source
      vp edit source --table sales
      vp edit source --table sales --force
    """
    from proto_pipe.io.config import SourceConfig
    from proto_pipe.cli.prompts import SourceConfigPrompter
    from proto_pipe.io.db import get_registry_hints, write_registry_types, table_exists
    from proto_pipe.io.migration import ColumnTypeMigration

    src_cfg = config_path_or_override("sources_config", sources_config)
    p_db = config_path_or_override("pipeline_db", pipeline_db)

    config = SourceConfig(src_cfg)

    if not table:
        sources = config.all()
        if not sources:
            click.echo("No sources configured yet. Run: vp new source")
            return
        name = questionary.select(
            "Which source would you like to edit?",
            choices=[s["name"] for s in sources],
        ).ask()
        if not name:
            click.echo("Cancelled.")
            return
        existing = config.get(name)
    else:
        existing = config.get_by_table(table)
        if not existing:
            click.echo(f"[error] No source found for table '{table}'")
            return

    target_table = existing["target_table"]
    click.echo(f"\n── Edit Source: {existing['name']} ──────────────────────────────")

    sample = None
    registry_hints: dict = {}

    with duckdb.connect(p_db) as conn:
        # Always fetch registry hints for this source — needed even when
        # the table doesn't exist yet (e.g. after vp table-reset)
        try:
            rows = conn.execute(
                """
                                SELECT column_name FROM column_type_registry
                                WHERE source_name = ?
                                ORDER BY column_name
                                """,
                [existing["name"]],
            ).fetchall()
            source_cols = [row[0] for row in rows]
            if source_cols:
                registry_hints = get_registry_hints(conn, source_cols)
        except Exception:
            pass

        # Sample from DB table when available
        if table_exists(conn, target_table):
            try:
                df = conn.execute(f'SELECT * FROM "{target_table}" LIMIT 20').df()
                sample = df[[c for c in df.columns if not c.startswith("_")]]
                # Extend registry hints to include any columns in sample
                # not already in registry (e.g. newly added columns)
                if sample is not None:
                    extra_cols = [c for c in sample.columns if c not in registry_hints]
                    if extra_cols:
                        extra_hints = get_registry_hints(conn, extra_cols)
                        registry_hints.update(extra_hints)
            except Exception:
                pass

    suggested = existing.get("patterns", ["*.csv"])[0]
    prompter = SourceConfigPrompter(
        sample_df=sample,
        registry_hints=registry_hints,
        existing_source=existing,
    )

    other_names = [n for n in config.names() if n != existing["name"]]

    if not prompter.run(other_names, suggested):
        click.echo("Cancelled.")
        return

    if prompter.confirmed_types:
        with duckdb.connect(p_db) as conn:
            if table_exists(conn, target_table):
                migration = ColumnTypeMigration(
                    conn,
                    target_table,
                    prompter.confirmed_types,
                    prompter.source.get("primary_key"),
                )
                if migration.has_changes():
                    issues = migration.pre_scan()
                    if issues and not force:
                        cols = sorted({i.column for i in issues})
                        click.echo(
                            f"\n[error] {len(issues)} row(s) cannot be cast to the "
                            f"new type(s) for: {', '.join(cols)}.\n"
                            f"Fix the data first, or use --force to flag and proceed."
                        )
                        return

                    result = migration.execute_force() if force else migration.execute()
                    if result.success:
                        click.echo(
                            f"[ok] Migrated '{target_table}': "
                            f"{result.rows_migrated} rows"
                            + (
                                f", {result.rows_flagged} flagged"
                                if result.rows_flagged
                                else ""
                            )
                        )
                        if result.columns_changed:
                            click.echo(
                                f"  Type changes: {', '.join(result.columns_changed)}"
                            )
                    else:
                        click.echo(f"[error] Migration failed: {result.error}")
                        return

            write_registry_types(
                conn, prompter.source["name"], prompter.confirmed_types
            )

    if prompter.source["name"] != existing["name"]:
        try:
            config.remove(existing["name"])
        except ValueError:
            pass

    config.add_or_update(prompter.source)
    click.echo(f"\n[ok] Source '{prompter.source['name']}' updated.")


@edit_cmd.command("report")
@click.option("--report", default=None, help="Report name to edit.")
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def edit_report(report, reports_config, pipeline_db):
    """Edit an existing report configuration.

    Re-runs the report wizard with existing values pre-filled.
    Press ESC at any step to go back.

    \b
    Examples:
      vp edit report
      vp edit report --report daily_sales_validation
    """
    from proto_pipe.io.config import ReportConfig
    from proto_pipe.checks.registry import CheckRegistry, CheckParamInspector
    from proto_pipe.checks.built_in import BUILT_IN_CHECKS
    from proto_pipe.checks.helpers import load_custom_checks_module
    from proto_pipe.io.db import init_check_registry_metadata
    from proto_pipe.cli.prompts import ReportConfigPrompter
    from proto_pipe.cli.scaffold import get_original_func, get_unconfigured_tables

    rep_cfg = config_path_or_override("reports_config", reports_config)
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    settings = load_settings()
    multi_select = settings.get("multi_select_params", True)

    config = ReportConfig(rep_cfg)

    if not report:
        names = config.names()
        if not names:
            click.echo("No reports configured yet. Run: vp new report")
            return
        report = questionary.select(
            "Which report would you like to edit?",
            choices=names,
        ).ask()
        if not report:
            click.echo("Cancelled.")
            return

    existing = config.get(report)
    if not existing:
        click.echo(f"[error] No report named '{report}' found.")
        return

    click.echo(f"\n── Edit Report: {existing['name']} ──────────────────────────────")
    click.echo("  Press ESC at any prompt to go back to the previous step.\n")

    check_registry = CheckRegistry()
    for name, func in BUILT_IN_CHECKS.items():
        check_registry.register(name, func)

    module_path = settings.get("custom_checks_module")
    if module_path:
        load_custom_checks_module(module_path, check_registry)

    # Include existing report's table in available tables
    other_reports = {"reports": [r for r in config.all() if r["name"] != report]}
    unconfigured = get_unconfigured_tables(p_db, other_reports)
    existing_table = existing.get("source", {}).get("table")
    available_tables = sorted(
        set(unconfigured + ([existing_table] if existing_table else []))
    )

    if not available_tables:
        click.echo("  No tables available.")
        return

    other_names = [n for n in config.names() if n != report]

    prompter = ReportConfigPrompter(
        check_registry=check_registry,
        p_db=p_db,
        multi_select=multi_select,
        existing_report=existing,
    )

    conn = duckdb.connect(p_db)
    init_check_registry_metadata(conn)
    for check_name in check_registry.available():
        original = get_original_func(check_name, check_registry)
        if original is not None:
            CheckParamInspector(original).write_to_db(conn, check_name)

    try:
        if not prompter.run(available_tables, other_names, conn):
            click.echo("Cancelled.")
            return
    finally:
        conn.close()

    updated = {
        "name": prompter.name,
        "source": {"type": "duckdb", "path": p_db, "table": prompter.table},
        "options": existing.get("options", {"parallel": False}),
    }
    if prompter.alias_map:
        updated["alias_map"] = prompter.alias_map
    updated["checks"] = prompter.check_entries

    if prompter.name != report:
        config.remove(report)
        config.add(updated)
    else:
        config.update(report, updated)

    click.echo(f"\n[ok] Report '{prompter.name}' updated.")


@edit_cmd.command("deliverable")
@click.option("--deliverable", default=None, help="Deliverable name to edit.")
@click.option("--deliverables-config", default=None)
@click.option("--reports-config", default=None)
@click.option("--sources-config", default=None)
@click.option("--sql-dir", default=None)
def edit_deliverable(deliverable, deliverables_config, reports_config, sources_config, sql_dir):
    """Edit an existing deliverable configuration.

    \b
    Examples:
      vp edit deliverable
      vp edit deliverable --deliverable carrier_a
    """
    from proto_pipe.io.config import DeliverableConfig
    from proto_pipe.io.config import load_settings
    from proto_pipe.cli.prompts import DeliverableConfigPrompter

    del_cfg = config_path_or_override("deliverables_config", deliverables_config)
    rep_cfg = config_path_or_override("reports_config", reports_config)
    src_cfg = config_path_or_override("sources_config", sources_config)
    settings = load_settings()
    sql_directory = sql_dir or settings["paths"].get("sql_dir", "sql")

    config = DeliverableConfig(del_cfg)
    rep_config = load_config(rep_cfg)
    src_config = load_config(src_cfg)

    if not deliverable:
        names = config.names()
        if not names:
            click.echo("No deliverables configured yet. Run: vp new deliverable")
            return
        deliverable = questionary.select(
            "Which deliverable would you like to edit?",
            choices=names,
        ).ask()
        if not deliverable:
            click.echo("Cancelled.")
            return

    existing = config.get(deliverable)
    if not existing:
        click.echo(f"[error] No deliverable named '{deliverable}' found.")
        return

    click.echo(
        f"\n── Edit Deliverable: {existing['name']} ──────────────────────────────"
    )

    available_reports = [r["name"] for r in rep_config.get("reports", [])]
    if not available_reports:
        click.echo("\n  No reports configured yet. Run: vp new report")
        return

    prompter = DeliverableConfigPrompter(
        rep_config=rep_config,
        src_config=src_config,
        sql_dir=sql_directory,
        existing_deliverable=existing,
    )

    name = prompter.prompt_name([n for n in config.names() if n != deliverable])
    if not name:
        click.echo("Cancelled.")
        return

    fmt = prompter.prompt_format()
    if not fmt:
        click.echo("Cancelled.")
        return

    filename_template = prompter.prompt_filename_template(name, fmt)
    if not filename_template:
        click.echo("Cancelled.")
        return

    selected_reports = prompter.prompt_reports(available_reports)
    if not selected_reports:
        click.echo("Cancelled.")
        return

    if fmt == "csv" and len(selected_reports) > 1:
        click.echo(
            "\n  [warn] Multiple reports selected for a CSV deliverable —"
            " you'll need to join them in your SQL file."
        )

    sql_file = prompter.prompt_sql(name, selected_reports)
    report_entries = prompter.prompt_report_entries(selected_reports, fmt)

    updated = {
        "name": name,
        "format": fmt,
        "filename_template": filename_template,
        "reports": report_entries,
    }
    if sql_file:
        updated["sql_file"] = sql_file

    if name != deliverable:
        config.remove(deliverable)
        config.add(updated)
    else:
        config.update(deliverable, updated)

    click.echo(f"\n[ok] Deliverable '{name}' updated.")


@edit_cmd.command("table")
@click.argument("table_name", required=False)
@click.option("--table", default=None, help="Table name (alternative to argument).")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def edit_table(table_name, table, pipeline_db):
    """Open a pipeline table in the interactive editor.

    Requires textual (uv add 'proto-pipe[tui]') for inline editing.
    Falls back to a read-only rich display if textual is not installed.

    \b
    Examples:
      vp edit table sales
      vp edit table --table sales
    """
    from proto_pipe.cli.commands.table import get_table_df, get_reviewer
    from proto_pipe.io.config import SourceConfig, load_settings
    from proto_pipe.reports.corrections import import_corrections
    import tempfile
    import os

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    name = table_name or table

    conn = duckdb.connect(p_db)
    try:
        all_tables = get_all_tables(conn)
        user_tables = [t for t in all_tables if t not in PIPELINE_TABLES]

        if not name:
            if not user_tables:
                click.echo("  No user tables found. Run: vp ingest")
                return
            name = questionary.select(
                "Which table would you like to edit?",
                choices=user_tables,
            ).ask()
            if not name:
                click.echo("Cancelled.")
                return

        if name not in all_tables:
            click.echo(f"[error] Table '{name}' not found.")
            return

        if name in PIPELINE_TABLES:
            click.echo(
                f"[error] '{name}' is a pipeline table and cannot be edited here.\n"
                f"Use 'vp flagged edit' to edit flagged rows."
            )
            return

        df = get_table_df(conn, name, limit=500)
        if df.empty:
            click.echo(f"'{name}' is empty.")
            return

        settings = load_settings()
        pk_col = None
        try:
            src_cfg = settings["paths"]["sources_config"]
            config = SourceConfig(src_cfg)
            source = config.get_by_table(name)
            if source:
                pk_col = source.get("primary_key")
        except Exception:
            pass

        reviewer = get_reviewer(edit=True)
        edited_df = reviewer.edit(
            df, title=f"{name} ({len(df)} rows)", pk_col=pk_col
        )

        if edited_df is not None and not edited_df.equals(df):
            if not pk_col:
                click.echo(
                    f"[warn] No primary key found for '{name}' — changes not saved.\n"
                    f"Set primary_key in sources_config.yaml to enable editing."
                )
                return

            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
                corrections_path = f.name
            try:
                edited_df.to_csv(corrections_path, index=False)
                result = import_corrections(conn, name, corrections_path, pk_col)
                click.echo(f"[ok] {result['updated']} row(s) updated in '{name}'")
            finally:
                os.unlink(corrections_path)
        else:
            click.echo("  No changes made.")

    finally:
        conn.close()


@edit_cmd.command("column-type")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option(
    "--diff",
    is_flag=True,
    default=False,
    help="Show only columns where sources disagree on the declared type.",
)
def edit_column_type(pipeline_db, diff):
    """Edit column type declarations across all sources.

    Shows the full column_type_registry as an editable grid, sorted
    alphabetically by column name. Useful for mass-correcting types
    shared across multiple sources — e.g. if 'policy_id' was registered
    as BIGINT but should be VARCHAR across all sources.

    Use --diff to show only columns where sources disagree, making it
    easy to find and bulk-resolve conflicting type declarations.

    After saving, you will be prompted to drop affected tables so the
    next vp ingest re-processes them with the corrected types.

    \b
    Examples:
      vp edit column-type
      vp edit column-type --diff
    """
    from proto_pipe.cli.commands.table import get_reviewer

    p_db = config_path_or_override("pipeline_db", pipeline_db)

    with duckdb.connect(p_db) as conn:
        try:
            df = conn.execute("""
                SELECT column_name, source_name, declared_type, recorded_at
                FROM column_type_registry
                ORDER BY column_name, source_name
            """).df()

            if diff:
                conflicting = conn.execute("""
                    SELECT column_name
                    FROM column_type_registry
                    GROUP BY column_name
                    HAVING count(DISTINCT declared_type) > 1
                """).df()["column_name"].tolist()

        except Exception as e:
            click.echo(f"[error] Could not read column_type_registry: {e}")
            return

    if df.empty:
        click.echo("No column types registered yet. Run: vp new source")
        return

    if diff:
        if not conflicting:
            click.echo("  No column type conflicts found — all sources agree.")
            return
        df = df[df["column_name"].isin(conflicting)].reset_index(drop=True).copy()
        click.echo(
            f"\n── Column Type Conflicts ──── {len(conflicting)} column(s), "
            f"{len(df)} entries ────────────────"
        )
        click.echo("  These columns have different declared types across sources.")
        click.echo("  Edit declared_type to resolve. Other columns are read-only.\n")
    else:
        click.echo(f"\n── Edit Column Types ──── {len(df)} entries ────────────────")
        click.echo("  Edit the declared_type column. Other columns are read-only.\n")

    from proto_pipe.constants import DUCKDB_TYPES

    reviewer = get_reviewer(edit=True)
    edited_df = reviewer.edit(
        df,
        title=f"column_type_registry ({len(df)} entries)",
        pk_col=("column_name", "source_name"),
        suggestions={"declared_type": DUCKDB_TYPES},
    )

    if edited_df is None or edited_df.equals(df):
        click.echo("  No changes made.")
        return

    # Identify changed rows — match on composite key (column_name, source_name)
    # since the same column name can appear across multiple sources.
    merged = edited_df.merge(
        df[["column_name", "source_name", "declared_type"]],
        on=["column_name", "source_name"],
        suffixes=("_new", "_orig"),
    )
    changed = merged[merged["declared_type_new"] != merged["declared_type_orig"]]
    if changed.empty:
        click.echo("  No changes detected.")
        return

    now = datetime.now(timezone.utc)
    with duckdb.connect(p_db) as conn:
        for _, row in changed.iterrows():
            conn.execute(
                """
                UPDATE column_type_registry
                SET declared_type = ?, recorded_at = ?
                WHERE column_name = ? AND source_name = ?
                """,
                [row["declared_type_new"], now, row["column_name"], row["source_name"]],
            )

    click.echo(f"\n[ok] {len(changed)} column type(s) updated.")

    # Show summary of what changed
    for _, row in changed.iterrows():
        click.echo(
            f"  {row['column_name']:<28} {row['declared_type_orig']} → {row['declared_type_new']}"
            f"  (source: {row['source_name']})"
        )

    # Prompt to drop affected tables for re-ingest
    affected_tables = (
        changed["source_name"]
        .map(lambda src: src)  # source_name maps to target_table via sources_config
        .unique()
        .tolist()
    )
    click.echo(f"\n  Affected sources: {', '.join(affected_tables)}")
    drop = questionary.confirm(
        "Drop affected tables now so vp ingest re-processes them with the new types?",
        default=False,
    ).ask()

    if not drop:
        click.echo(
            "  Skipped. Run: vp table-reset --report <name>  to drop tables manually."
        )
        return

    with duckdb.connect(p_db) as conn:
        for source_name in affected_tables:
            # Resolve target_table from sources_config
            try:
                src_cfg = config_path_or_override("sources_config", None)
                from proto_pipe.io.config import SourceConfig

                source = SourceConfig(src_cfg).get(source_name)
                target_table = source["target_table"] if source else source_name
            except Exception:
                target_table = source_name

            try:
                conn.execute(f'DROP TABLE IF EXISTS "{target_table}"')
                conn.execute(
                    "DELETE FROM ingest_state WHERE table_name = ?", [target_table]
                )
                click.echo(
                    f"  [ok] Dropped '{target_table}' — will re-ingest on next vp ingest"
                )
            except Exception as e:
                click.echo(f"  [error] Could not drop '{target_table}': {e}")


def edit_commands(cli: click.Group) -> None:
    cli.add_command(edit_cmd)
    cli.add_command(edit_table)
    cli.add_command(edit_deliverable)
    cli.add_command(edit_column_type)
    cli.add_command(edit_report)
    cli.add_command(edit_source)
