"""vp delete — delete pipeline resources."""

import click
import duckdb
import questionary

from proto_pipe.io.db import get_all_tables
from proto_pipe.io.config import config_path_or_override
from proto_pipe.constants import PIPELINE_TABLES


@click.group("delete", context_settings={"max_content_width": 120})
def delete_cmd():
    """Delete pipeline resources.

    \b
    Examples:
      vp delete source
      vp delete report
      vp delete deliverable
      vp delete table
    """
    pass


@delete_cmd.command("source")
@click.option(
    "--table", default=None, help="Source table name to delete (skips multi-select)."
)
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
def delete_source(table, yes, pipeline_db, sources_config):
    """Remove one or more sources and all their associated data.

    Drops the DB table, removes the config entry, clears ingest_state,
    source_block, and source_pass entries. column_type_registry
    entries are kept since other sources may share those columns.

    \b
    Examples:
      vp delete source
      vp delete source --table sales
      vp delete source --table sales --yes
    """
    from proto_pipe.io.config import SourceConfig
    from proto_pipe.io.db import table_exists
    from proto_pipe.pipelines.query import query_delete_source_impact
    from proto_pipe.cli.prompts import prompt_delete_impact

    src_cfg = config_path_or_override("sources_config", sources_config)
    p_db = config_path_or_override("pipeline_db", pipeline_db)

    config = SourceConfig(src_cfg)
    all_sources = config.all()

    if not all_sources:
        click.echo("No sources configured.")
        return

    if table:
        source = config.get_by_table(table)
        if not source:
            click.echo(f"[error] No source found for table '{table}'")
            return
        selected = [source]
    else:
        names = questionary.checkbox(
            "Select sources to delete:",
            choices=[s["name"] for s in all_sources],
        ).ask()
        if not names:
            click.echo("Cancelled.")
            return
        selected = [config.get(n) for n in names]

    # Show selected source names
    click.echo()
    for source in selected:
        click.echo(f"  {source['name']:<28} table: {source['target_table']}")

    # Connect before confirmation — needed for impact counts
    conn = duckdb.connect(p_db)
    try:
        # Build and show impact summary, prompt for confirmation
        all_impact_rows = []
        for source in selected:
            all_impact_rows.extend(
                query_delete_source_impact(conn, source["target_table"])
            )

        if not prompt_delete_impact(all_impact_rows, yes=yes):
            return

        for source in selected:
            name = source["name"]
            target_table = source["target_table"]
            click.echo(f"\n── {name} ──────────────────────────────────────")
            try:
                from proto_pipe.io.db import delete_source_cascade

                result = delete_source_cascade(conn, target_table)
                if result.table_dropped:
                    click.echo(f"  [ok] Dropped table '{target_table}'")
                else:
                    click.echo(f"  [skip] Table '{target_table}' not found in DB")
                if result.ingest_state_cleared:
                    click.echo(
                        f"  [ok] Cleared {result.ingest_state_cleared} ingest_state entry/entries"
                    )
                if result.source_block_cleared:
                    click.echo(
                        f"  [ok] Cleared {result.source_block_cleared} source_block entry/entries"
                    )
                if result.source_pass_cleared:
                    click.echo(
                        f"  [ok] Cleared {result.source_pass_cleared} source_pass entry/entries"
                    )

                config.remove(name)
                click.echo(f"  [ok] Removed '{name}' from sources_config.yaml")

            except Exception as e:
                click.echo(f"  [error] {name}: {e}")

    finally:
        conn.close()

    click.echo(f"\n[ok] {len(selected)} source(s) deleted.")


@delete_cmd.command("report")
@click.option("--report", default=None, help="Report name to delete.")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--reports-config", default=None, help="Override reports config path.")
def delete_report(report, yes, pipeline_db, reports_config):
    """Remove a report configuration.

    Drops the report table, removes the config entry, clears
    validation_block and validation_pass entries.

    \b
    Examples:
      vp delete report
      vp delete report --report daily_sales_validation
    """
    from proto_pipe.io.config import ReportConfig
    from proto_pipe.io.db import delete_report_cascade
    from proto_pipe.pipelines.query import query_delete_report_impact
    from proto_pipe.cli.prompts import prompt_delete_impact

    rep_cfg = config_path_or_override("reports_config", reports_config)
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    config = ReportConfig(rep_cfg)

    if not report:
        names = config.names()
        if not names:
            click.echo("No reports configured.")
            return
        report = questionary.select(
            "Which report would you like to delete?",
            choices=names,
        ).ask()
        if not report:
            click.echo("Cancelled.")
            return

    existing = config.get(report)
    if not existing:
        click.echo(f"[error] No report named '{report}' found.")
        return

    target_table = existing.get("target_table", report)

    # Connect before confirmation — needed for impact counts
    conn = duckdb.connect(p_db)
    try:
        impact = query_delete_report_impact(conn, report, target_table)

        if not prompt_delete_impact(impact, yes=yes):
            return

        result = delete_report_cascade(conn, report, target_table)
        if result.table_dropped:
            click.echo(f"  [ok] Dropped report table '{target_table}'")
        else:
            click.echo(f"  [skip] Report table '{target_table}' not found in DB")
        if result.validation_block_cleared:
            click.echo(
                f"  [ok] Cleared {result.validation_block_cleared} validation_block entry/entries"
            )
        if result.validation_pass_cleared:
            click.echo(
                f"  [ok] Cleared {result.validation_pass_cleared} validation_pass entry/entries"
            )

    finally:
        conn.close()

    config.remove(report)
    click.echo(f"[ok] Report '{report}' removed from reports_config.yaml")


@delete_cmd.command("deliverable")
@click.option("--deliverable", default=None, help="Deliverable name to delete.")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--deliverables-config", default=None)
def delete_deliverable(deliverable, yes, deliverables_config):
    """Remove a deliverable configuration.

    Only removes the config entry — does not delete any SQL files.

    \b
    Examples:
      vp delete deliverable
      vp delete deliverable --deliverable carrier_a
    """
    from proto_pipe.io.config import DeliverableConfig

    del_cfg = config_path_or_override("deliverables_config", deliverables_config)
    config = DeliverableConfig(del_cfg)

    if not deliverable:
        names = config.names()
        if not names:
            click.echo("No deliverables configured.")
            return
        deliverable = questionary.select(
            "Which deliverable would you like to delete?",
            choices=names,
        ).ask()
        if not deliverable:
            click.echo("Cancelled.")
            return

    existing = config.get(deliverable)
    if not existing:
        click.echo(f"[error] No deliverable named '{deliverable}' found.")
        return

    sql_file = existing.get("sql_file")

    try:
        if not yes:
            note = f" (SQL file '{sql_file}' will NOT be deleted)" if sql_file else ""
            click.confirm(
                f"Delete deliverable '{deliverable}'?{note} This cannot be undone.",
                abort=True,
            )

        config.remove(deliverable)
        click.echo(
            f"[ok] Deliverable '{deliverable}' removed from deliverables_config.yaml"
        )
        if sql_file:
            click.echo(f"  Note: SQL file '{sql_file}' was not deleted.")

    except click.Abort:
        click.echo("\n  Cancelled.")


@delete_cmd.command("table")
@click.argument("table_name", required=False)
@click.option("--table", default=None, help="Table name (alternative to argument).")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def delete_table(table_name, table, yes, pipeline_db):
    """Drop a non-pipeline table from the DB.

    Does not modify any config files. Use 'vp delete source' to
    remove both the table and its source config entry.

    \b
    Examples:
      vp delete table sales
      vp delete table --table sales --yes
    """
    from proto_pipe.pipelines.query import query_delete_table_impact
    from proto_pipe.cli.prompts import prompt_delete_impact

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    name = table_name or table

    conn = duckdb.connect(p_db)
    try:
        all_tables = get_all_tables(conn)
        user_tables = [t for t in all_tables if t not in PIPELINE_TABLES]

        if not name:
            if not user_tables:
                click.echo("  No user tables found.")
                return
            name = questionary.select(
                "Which table would you like to drop?",
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
                f"[error] '{name}' is a pipeline table and cannot be dropped here."
            )
            return

        impact = query_delete_table_impact(conn, name)

        if not prompt_delete_impact(impact, yes=yes):
            return

        conn.execute(f'DROP TABLE "{name}"')
        click.echo(f"[ok] Table '{name}' dropped.")
        click.echo(
            f"  Note: sources_config.yaml was not updated. "
            f"Run 'vp delete source --table {name}' to remove the config entry too."
        )

    finally:
        conn.close()


def delete_commands(cli: click.Group) -> None:
    cli.add_command(delete_cmd)
