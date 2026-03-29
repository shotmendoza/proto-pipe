"""CLI package — assembles all command groups onto the root `cli` group."""

import click

from .data import data_commands
from .flagged import flagged_commands
from .quickstart import setup_commands
from .reports import reports_commands
from .scaffold import scaffold_commands
from .validation import validation_commands
from .table import table_commands


@click.group()
def cli():
    """Validation pipeline — manage your config and pipeline."""
    pass


setup_commands(cli)
data_commands(cli)
validation_commands(cli)
reports_commands(cli)
flagged_commands(cli)
scaffold_commands(cli)
table_commands(cli)
