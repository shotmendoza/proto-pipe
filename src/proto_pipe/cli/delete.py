"""vp delete — delete pipeline resources.

All subcommands are stubs until Session C.
"""

import click


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
@click.option("--table", default=None, help="Source table name to delete.")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation.")
def delete_source(table, yes):
    """Remove a source: drops table, clears config, ingest_log, and flagged_rows."""
    click.echo("[todo] vp delete source — coming in Session C")


@delete_cmd.command("report")
@click.option("--report", default=None, help="Report name to delete.")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation.")
def delete_report(report, yes):
    """Remove a report configuration."""
    click.echo("[todo] vp delete report — coming in Session C")


@delete_cmd.command("deliverable")
@click.option("--deliverable", default=None, help="Deliverable name to delete.")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation.")
def delete_deliverable(deliverable, yes):
    """Remove a deliverable configuration."""
    click.echo("[todo] vp delete deliverable — coming in Session C")


@delete_cmd.command("table")
@click.argument("table_name", required=False)
@click.option("--table", default=None, help="Table name (alternative to argument).")
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def delete_table(table_name, table, yes, pipeline_db):
    """Drop a non-pipeline table from the DB."""
    click.echo("[todo] vp delete table — coming in Session C")


def delete_commands(cli: click.Group) -> None:
    cli.add_command(delete_cmd)
