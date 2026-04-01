"""vp edit — edit existing pipeline resources.

All subcommands are stubs until Session C.
"""

import click


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
def edit_source(table):
    """Edit an existing source configuration and migrate types if needed."""
    click.echo("[todo] vp edit source — coming in Session C")


@edit_cmd.command("report")
@click.option("--report", default=None, help="Report name to edit.")
def edit_report(report):
    """Edit an existing report configuration."""
    click.echo("[todo] vp edit report — coming in Session C")


@edit_cmd.command("deliverable")
@click.option("--deliverable", default=None, help="Deliverable name to edit.")
def edit_deliverable(deliverable):
    """Edit an existing deliverable configuration."""
    click.echo("[todo] vp edit deliverable — coming in Session C")


@edit_cmd.command("table")
@click.argument("table_name", required=False)
@click.option("--table", default=None, help="Table name (alternative to argument).")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def edit_table(table_name, table, pipeline_db):
    """Open a pipeline table in the interactive editor."""
    click.echo("[todo] vp edit table — coming in Session C")


def edit_commands(cli: click.Group) -> None:
    cli.add_command(edit_cmd)
