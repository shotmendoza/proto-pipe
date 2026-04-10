"""Table commands — view, edit, and export pipeline tables."""

import click
import duckdb
import pandas as pd

from proto_pipe.io.config import config_path_or_override, load_config


def get_table_df(conn: duckdb.DuckDBPyConnection, table: str, limit: int):
    """Return a DataFrame for the given table."""
    return conn.execute(f'SELECT * FROM "{table}" LIMIT {limit}').df()


def _display_rich_table(df, title: str) -> None:
    """Fallback table display when Textual is not installed.

    Used only when textual is unavailable. For full horizontal scrolling
    install textual: uv add 'proto-pipe[tui]'
    """
    from rich.console import Console
    from rich.table import Table

    console = Console()
    table = Table(
        title=title,
        show_header=True,
        header_style="bold cyan",
        show_lines=True,
        row_styles=["", "dim"],
    )

    for col in df.columns:
        table.add_column(str(col), no_wrap=True)

    # astype(object) before fillna("") required for nullable DuckDB types (Int64 etc.)
    rows = df.astype(object).fillna("").astype(str).values.tolist()
    for row in rows:
        table.add_row(*row)

    with console.pager(styles=True):
        console.print(table)
        console.print(f"[dim]{len(df)} row(s) shown[/dim]")


# ---------------------------------------------------------------------------
# ReviewInterface protocol
# ---------------------------------------------------------------------------
class ReviewInterface:
    """Protocol for table review/edit interfaces.

    Implement this to swap out the TUI backend without changing CLI commands.
    """

    def show(self, df, title: str, pk_col: str | tuple | None = None):
        raise NotImplementedError

    def edit(
        self,
        df: pd.DataFrame,
        title: str,
        pk_col: str | tuple | None = None,
        suggestions: dict[str, list[str]] | None = None,
    ) -> pd.DataFrame:
        """Show editable table. Returns DataFrame with user edits applied.

        :param suggestions: Optional {column_name: [valid_values]} mapping.
            When provided, the TUI shows an autocomplete dropdown for that
            column while editing. Falls back to plain input if not set.
        """
        raise NotImplementedError


class RichReview(ReviewInterface):
    """Read-only rich table display."""

    def show(self, df, title: str, pk_col: str | tuple | None = None):
        _display_rich_table(df, title)

    def edit(
        self,
        df: pd.DataFrame,
        title: str,
        pk_col: str | tuple | None = None,
        suggestions: dict[str, list[str]] | None = None,
    ) -> pd.DataFrame:
        from rich.console import Console

        Console().print(
            "[yellow]Interactive editing requires textual.[/yellow]\n"
            "Install it with: uv add 'proto-pipe[tui]'"
        )
        self.show(df, title)
        return df


class TextualReview(ReviewInterface):
    """Interactive TUI table editor backed by textual."""

    def _make_app(
        self,
        df,
        title: str,
        pk_col: str | tuple | None,
        editable: bool,
        suggestions: dict[str, list[str]] | None = None,
    ):
        """Build and return the Textual app without running it — used for testing."""
        from textual.app import App, ComposeResult
        from textual.widgets import DataTable, Footer, Header
        from textual.binding import Binding

        _suggestions = suggestions or {}
        changes: dict[tuple, str] = {}

        class TableApp(App):
            BINDINGS = [
                Binding("ctrl+s", "save", "Save", show=True),
                Binding("ctrl+c", "copy_cell", "Copy", show=True),
                Binding("escape", "quit_no_save", "Quit", show=True),
            ]

            def compose(self) -> ComposeResult:
                yield Header()
                yield DataTable()
                yield Footer()

            def on_mount(self) -> None:
                table = self.query_one(DataTable)
                table.cursor_type = "cell" if editable else "row"
                table.zebra_stripes = True

                for col in df.columns:
                    is_pk = col == pk_col or (
                        isinstance(pk_col, tuple) and col in pk_col
                    )
                    if is_pk:
                        table.add_column(
                            f"[bold yellow]{col}[/bold yellow]", key=col, width=None
                        )
                    else:
                        table.add_column(str(col), key=col, width=None)

                rows = df.astype(object).fillna("").astype(str).values.tolist()
                for row in rows:
                    table.add_row(*row)

                self.title = title
                self.sub_title = (
                    "Ctrl+S save  |  Ctrl+C copy  |  Esc quit  |  ← → scroll"
                    if editable
                    else "Ctrl+C copy  |  Esc quit  |  ← → scroll"
                )

            def on_data_table_cell_selected(self, event: DataTable.CellSelected) -> None:
                if not editable:
                    return
                from textual.widgets import Input
                from textual.suggester import SuggestFromList
                from textual.screen import ModalScreen

                row_idx = event.coordinate.row
                col_idx = event.coordinate.column
                col_name = str(df.columns[col_idx])
                current_val = str(df.iloc[row_idx, col_idx] or "")
                col_suggestions = _suggestions.get(col_name)

                class EditCell(ModalScreen):
                    def compose(self) -> ComposeResult:
                        if col_suggestions:
                            yield Input(
                                value=current_val,
                                placeholder=f"Edit {col_name}",
                                suggester=SuggestFromList(
                                    col_suggestions, case_sensitive=False
                                ),
                            )
                        else:
                            yield Input(
                                value=current_val,
                                placeholder=f"Edit {col_name}",
                            )

                    def on_input_submitted(self, sub_event: Input.Submitted) -> None:
                        self.dismiss(sub_event.value)

                def handle_edit(new_val):
                    if new_val is not None:
                        changes[(row_idx, col_name)] = new_val
                        table = self.query_one(DataTable)
                        table.update_cell_at(event.coordinate, new_val)

                self.push_screen(EditCell(), handle_edit)

            def action_copy_cell(self) -> None:
                """Copy the current cell value to the system clipboard."""
                table = self.query_one(DataTable)
                coord = table.cursor_coordinate
                if coord is None:
                    return
                col_name = str(df.columns[coord.column])
                # Prefer pending edit value, fall back to original
                val = changes.get((coord.row, col_name))
                if val is None:
                    val = str(df.iloc[coord.row, coord.column] or "")
                self.app.copy_to_clipboard(val)
                self.notify(
                    f"Copied: {val[:40]}{'...' if len(val) > 40 else ''}"
                )

            def action_save(self) -> None:
                self.exit(result=changes)

            def action_quit_no_save(self) -> None:
                self.exit(result=None)

        app = TableApp()
        app._result = None
        app.pk_col = pk_col
        return app

    def _run(
        self,
        df,
        title: str,
        pk_col: str | tuple | None,
        editable: bool,
        suggestions: dict[str, list[str]] | None = None,
    ):
        app = self._make_app(df, title, pk_col, editable, suggestions)
        result = app.run()
        return result

    def show(self, df, title: str, pk_col: str | tuple | None = None):
        self._run(df, title, pk_col, editable=False)
        return df

    def edit(
        self,
        df,
        title: str,
        pk_col: str | tuple | None = None,
        suggestions: dict[str, list[str]] | None = None,
    ):
        edited_df = df.copy()
        changes = self._run(df, title, pk_col, editable=True, suggestions=suggestions)
        if changes:
            for (row_idx, col_name), value in changes.items():
                edited_df.at[row_idx, col_name] = value
        return edited_df


def get_reviewer(edit: bool = False) -> ReviewInterface:
    """Return the best available reviewer.

    Always tries TextualReview first — it provides proper horizontal and
    vertical scrolling for both viewing and editing. Falls back to RichReview
    if textual is not installed.

    Install textual for the best experience: uv add 'proto-pipe[tui]'
    """
    try:
        import textual  # noqa
        return TextualReview()
    except ImportError:
        return RichReview()


# ---------------------------------------------------------------------------
# vp table
# ---------------------------------------------------------------------------


@click.command("table")
@click.argument("table_name", required=False)
@click.option(
    "--edit", is_flag=True, default=False, help="Open table in interactive editor."
)
@click.option("--export", default=None, help="Export table to CSV at this path.")
@click.option("--limit", default=None, show_default=True, help="Max rows to display.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def table_cmd(table_name, edit, export, limit, pipeline_db):
    """[Removed] Use vp view table or vp edit table instead."""
    import sys

    click.echo(
        "[error] 'vp table' has been removed.\n"
        "        To view a table:      vp view table\n"
        "        To edit a table:      vp edit table\n"
        "        To edit flagged rows: vp errors source edit"
    )
    sys.exit(1)


def table_commands(cli: click.Group) -> None:
    cli.add_command(table_cmd)
