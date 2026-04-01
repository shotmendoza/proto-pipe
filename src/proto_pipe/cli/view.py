"""vp view — view pipeline resources.

All subcommands are stubs until Session D.
"""

import click


@click.group("view", context_settings={"max_content_width": 120})
def view_cmd():
    """View pipeline resource data.

    \b
    Examples:
      vp view source
      vp view report
      vp view deliverable
      vp view table
    """
    pass


@view_cmd.command("source")
@click.argument("table_name", required=False)
@click.option("--table", default=None, help="Source table name.")
@click.option("--export", default=None, type=click.Choice(["csv", "term"]))
@click.option("--limit", default=500, show_default=True)
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def view_source(table_name, table, export, limit, pipeline_db):
    """View ingested rows for a source table."""
    click.echo("[todo] vp view source — coming in Session D")


@view_cmd.command("report")
@click.argument("report_name", required=False)
@click.option("--report", default=None, help="Report name.")
@click.option("--export", default=None, type=click.Choice(["csv", "term"]))
@click.option("--limit", default=500, show_default=True)
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def view_report(report_name, report, export, limit, pipeline_db):
    """View the data table a report runs against."""
    click.echo("[todo] vp view report — coming in Session D")


@view_cmd.command("deliverable")
@click.argument("deliverable_name", required=False)
@click.option("--deliverable", default=None, help="Deliverable name.")
@click.option("--export", default=None, type=click.Choice(["csv", "term"]))
@click.option("--limit", default=500, show_default=True)
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def view_deliverable(deliverable_name, deliverable, export, limit, pipeline_db):
    """Preview deliverable output (staging view before export)."""
    click.echo("[todo] vp view deliverable — coming in Session D")


@view_cmd.command("table")
@click.argument("table_name", required=False)
@click.option("--table", default=None, help="Table name (alternative to argument).")
@click.option("--export", default=None, type=click.Choice(["csv", "term"]))
@click.option("--limit", default=500, show_default=True)
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def view_table(table_name, table, export, limit, pipeline_db):
    """View any pipeline table with rich display."""
    click.echo("[todo] vp view table — coming in Session D")


def view_commands(cli: click.Group) -> None:
    cli.add_command(view_cmd)
