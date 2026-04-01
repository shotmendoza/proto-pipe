"""vp new — create new pipeline resources.

Wraps the existing scaffold commands under a unified group.
Command logic lives in scaffold.py until Session B moves it here.
"""

import click


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


def _register(cli_group: click.Group) -> None:
    """Register all vp new subcommands onto the group."""
    from proto_pipe.cli.scaffold import (
        new_source,
        new_report,
        new_deliverable,
        new_view,
        new_macro,
        new_sql,
    )

    cli_group.add_command(new_source, name="source")
    cli_group.add_command(new_report, name="report")
    cli_group.add_command(new_deliverable, name="deliverable")
    cli_group.add_command(new_view, name="view")
    cli_group.add_command(new_macro, name="macro")
    cli_group.add_command(new_sql, name="sql")


_register(new_cmd)


def new_commands(cli: click.Group) -> None:
    cli.add_command(new_cmd)
