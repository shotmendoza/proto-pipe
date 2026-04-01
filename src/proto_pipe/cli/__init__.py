"""CLI package — assembles all command groups onto the root `cli` group."""

import click

from .data import data_commands
from .delete import delete_commands
from .edit import edit_commands
from .flagged import flagged_commands
from .funcs import funcs_commands
from .new import new_commands
from .quickstart import setup_commands
from .reports import reports_commands
from .scaffold import scaffold_commands
from .table import table_commands
from .validation import validation_commands
from .view import view_commands


@click.group(context_settings={"max_content_width": 120})
def cli():
    """Validation pipeline — manage your config and pipeline."""
    pass


setup_commands(cli)
data_commands(cli)
validation_commands(cli)
reports_commands(cli)
new_commands(cli)
edit_commands(cli)
delete_commands(cli)
view_commands(cli)
flagged_commands(cli)
funcs_commands(cli)
scaffold_commands(cli)
table_commands(cli)
