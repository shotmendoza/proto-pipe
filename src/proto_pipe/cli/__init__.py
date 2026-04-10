"""CLI package — assembles all command groups onto the root `cli` group."""

import click

from .data import data_commands
from proto_pipe.cli.commands.delete import delete_commands
from proto_pipe.cli.commands.edit import edit_commands
from .errors import errors_commands
from proto_pipe.cli.commands.funcs import funcs_commands
from proto_pipe.cli.commands.new import new_commands
from proto_pipe.cli.commands.refresh import refresh_commands
from .quickstart import setup_commands
from .reports import reports_commands
from .status import status_commands
from proto_pipe.cli.commands.table import table_commands
from proto_pipe.cli.commands.validation import validation_commands
from proto_pipe.cli.commands.view import view_commands


# ---------------------------------------------------------------------------
# Custom help formatter — Pipeline / Inspect / Configure / Maintain sections
# ---------------------------------------------------------------------------

_COMMAND_SECTIONS = {
    "Pipeline": ["ingest", "validate", "deliver", "run-all"],
    "Inspect": ["status", "errors", "view", "funcs", "table"],
    "Configure": ["new", "edit", "delete", "config"],
    "Maintain": ["init", "refresh", "docs"],
}

# Build reverse lookup: command_name → section
_CMD_TO_SECTION = {}
for section, cmds in _COMMAND_SECTIONS.items():
    for cmd in cmds:
        _CMD_TO_SECTION[cmd] = section


class SectionHelpFormatter(click.HelpFormatter):
    """Overrides format_help to group commands under section headers."""
    pass


class SectionGroup(click.Group):
    """Click Group that displays commands under section headers in --help."""

    def format_commands(self, ctx, formatter):
        # Collect all commands
        commands = []
        for subcommand in self.list_commands(ctx):
            cmd = self.commands.get(subcommand)
            if cmd is None or cmd.hidden:
                continue
            help_text = cmd.get_short_help_str(limit=formatter.width)
            commands.append((subcommand, help_text))

        if not commands:
            return

        # Group into sections
        sections: dict[str, list[tuple[str, str]]] = {}
        unsectioned = []
        for name, help_text in commands:
            section = _CMD_TO_SECTION.get(name)
            if section:
                sections.setdefault(section, []).append((name, help_text))
            else:
                unsectioned.append((name, help_text))

        # Render each section in defined order
        for section_name in _COMMAND_SECTIONS:
            if section_name not in sections:
                continue
            order = _COMMAND_SECTIONS[section_name]
            sorted_cmds = sorted(
                sections[section_name],
                key=lambda item: order.index(item[0]) if item[0] in order else len(order),
            )
            with formatter.section(section_name):
                formatter.write_dl(sorted_cmds)

        if unsectioned:
            with formatter.section("Other"):
                formatter.write_dl(unsectioned)


@click.group(cls=SectionGroup, context_settings={"max_content_width": 120})
def cli():
    """vp — validation pipeline CLI."""
    pass


setup_commands(cli)
data_commands(cli)
validation_commands(cli)
reports_commands(cli)
new_commands(cli)
edit_commands(cli)
delete_commands(cli)
refresh_commands(cli)
view_commands(cli)
errors_commands(cli)
status_commands(cli)
funcs_commands(cli)
table_commands(cli)
