"""Table commands — view, edit, and export pipeline tables."""

from pathlib import Path

import click
import duckdb

from proto_pipe.io.config import config_path_or_override, load_config, load_settings
from proto_pipe.constants import PIPELINE_TABLES
from proto_pipe.io.db import get_all_tables


def _get_table_df(conn: duckdb.DuckDBPyConnection, table: str, limit: int):
    """Return a DataFrame for the given table."""
    return conn.execute(f'SELECT * FROM "{table}" LIMIT {limit}').df()


def _display_rich_table(df, title: str) -> None:
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
        table.add_column(str(col), overflow="fold")

    rows = df.astype(object).fillna("").astype(str).values.tolist()
    for row in rows:
        table.add_row(*row)

    with console.pager():
        console.print(table)
        console.print(f"[dim]{len(df)} row(s) shown[/dim]")


# ---------------------------------------------------------------------------
# ReviewInterface protocol
# ---------------------------------------------------------------------------
class ReviewInterface:
    """Protocol for table review/edit interfaces.

    Implement this to swap out the TUI backend without changing CLI commands.
    """

    def show(self, df, title: str, pk_col: str | None = None):
        raise NotImplementedError

    def edit(self, df, title: str, pk_col: str | None = None):
        """Show editable table. Returns DataFrame with user edits applied."""
        raise NotImplementedError


class RichReview(ReviewInterface):
    """Read-only rich table display."""

    def show(self, df, title: str, pk_col: str | None = None):
        _display_rich_table(df, title)

    def edit(self, df, title: str, pk_col: str | None = None):
        from rich.console import Console

        Console().print(
            "[yellow]Interactive editing requires textual.[/yellow]\n"
            "Install it with: uv add 'proto-pipe[tui]'"
        )
        self.show(df, title)
        return df


class TextualReview(ReviewInterface):
    """Interactive TUI table editor backed by textual."""

    def _make_app(self, df, title: str, pk_col: str | None, editable: bool):
        """Build and return the Textual app without running it — used for testing."""
        from textual.app import App, ComposeResult
        from textual.widgets import DataTable, Footer, Header
        from textual.binding import Binding

        changes: dict[tuple, str] = {}

        class TableApp(App):
            BINDINGS = [
                Binding("ctrl+s", "save", "Save", show=True),
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
                    if col == pk_col:
                        table.add_column(
                            f"[bold yellow]{col}[/bold yellow]", key=col, width=None
                        )
                    else:
                        table.add_column(str(col), key=col, width=None)

                rows = df.fillna("").astype(str).values.tolist()
                for row in rows:
                    table.add_row(*row)

                self.title = title
                self.sub_title = (
                    "Ctrl+S to save  |  Esc to quit  |  ← → scroll"
                    if editable
                    else "Esc to quit  |  ← → scroll"
                )

            def on_data_table_cell_selected(self, event: DataTable.CellSelected) -> None:
                if not editable:
                    return
                from textual.widgets import Input
                from textual.screen import ModalScreen

                row_idx = event.coordinate.row
                col_idx = event.coordinate.column
                col_name = str(df.columns[col_idx])
                current_val = str(df.iloc[row_idx, col_idx] or "")

                class EditCell(ModalScreen):
                    def compose(self) -> ComposeResult:
                        yield Input(value=current_val, placeholder=f"Edit {col_name}")

                    def on_input_submitted(self, sub_event: Input.Submitted) -> None:
                        self.dismiss(sub_event.value)

                def handle_edit(new_val):
                    if new_val is not None:
                        changes[(row_idx, col_name)] = new_val
                        table = self.query_one(DataTable)
                        table.update_cell_at(event.coordinate, new_val)

                self.push_screen(EditCell(), handle_edit)

            def action_save(self) -> None:
                self.exit(result=changes)

            def action_quit_no_save(self) -> None:
                self.exit(result=None)

        app = TableApp()
        app._result = None
        app.pk_col = pk_col
        return app

    def _run(self, df, title: str, pk_col: str | None, editable: bool):
        app = self._make_app(df, title, pk_col, editable)
        result = app.run()
        return result

    def show(self, df, title: str, pk_col: str | None = None):
        self._run(df, title, pk_col, editable=False)
        return df

    def edit(self, df, title: str, pk_col: str | None = None):
        edited_df = df.copy()
        changes = self._run(df, title, pk_col, editable=True)
        if changes:
            for (row_idx, col_name), value in changes.items():
                edited_df.at[row_idx, col_name] = value
        return edited_df


def get_reviewer(edit: bool = False) -> ReviewInterface:
    """Return the best available reviewer."""
    if edit:
        try:
            import textual  # noqa

            return TextualReview()
        except ImportError:
            return RichReview()
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
    """View, edit, or export any pipeline table.

    \b
    Examples:
      vp table # select table interactively
      vp table sales # view sales table
      vp table flagged_rows --edit # edit flagged rows inline
      vp table ingest_log --export ingest_log.csv
    """
    import questionary

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)

    try:
        all_tables = get_all_tables(conn)

        if not all_tables:
            click.echo("  No tables found in the pipeline DB. Run: vp ingest")
            return

        # If no table specified, show selection menu
        if not table_name:
            # Split into user tables and infrastructure tables for clarity
            user_tables = [t for t in all_tables if t not in PIPELINE_TABLES]
            infra_tables = [t for t in all_tables if t in PIPELINE_TABLES]

            choices = []
            if user_tables:
                choices.append(questionary.Separator("── Data Tables ──"))
                choices.extend(user_tables)
            if infra_tables:
                choices.append(questionary.Separator("── Pipeline Tables ──"))
                choices.extend(infra_tables)

            table_name = questionary.select(
                "Which table would you like to view?",
                choices=choices,
            ).ask()

            if not table_name:
                click.echo("Cancelled.")
                return

        # Verify table exists
        if table_name not in all_tables:
            click.echo(f"[error] Table '{table_name}' not found.")
            click.echo(f"Available: {', '.join(all_tables)}")
            return

        df = _get_table_df(conn, table_name, limit or 100)

        if df.empty:
            click.echo(f"'{table_name}' is empty.")
            return

        # Export path
        if export:
            # Fetch without limit for export
            export_df = conn.execute(
                f'SELECT * FROM "{table_name}"'
                + (f" LIMIT {limit}" if limit is not None else "")
            ).df()
            # Resolve path — if no directory given, use output_dir from settings
            export_path = Path(export)
            if not export_path.is_absolute() and export_path.parent == Path("."):
                try:
                    out_dir = load_settings()["paths"]["output_dir"]
                    export_path = Path(out_dir) / export_path
                except Exception:
                    pass
            export_path.parent.mkdir(parents=True, exist_ok=True)
            export_df.to_csv(export_path, index=False)
            click.echo(f"[ok] {len(export_df)} row(s) exported to {export_path}")
            return

        # Get primary key for this table if it's a user table

        settings = load_settings()
        pk_col = None
        try:
            src_cfg = settings["paths"]["sources_config"]
            sources = load_config(src_cfg).get("sources", [])
            source = next((s for s in sources if s["target_table"] == table_name), None)
            if source:
                pk_col = source.get("primary_key")
        except Exception:
            pass

        reviewer = get_reviewer(edit=edit)

        if edit:
            edited_df = reviewer.edit(
                df, title=f"{table_name} ({len(df)} rows)", pk_col=pk_col
            )
            if edited_df is not None and not edited_df.equals(df):
                from proto_pipe.reports.corrections import import_corrections
                import tempfile
                import os

                with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
                    corrections_path = f.name
                try:
                    edited_df.to_csv(corrections_path, index=False)
                    if pk_col:
                        import_corrections(conn, table_name, corrections_path, pk_col)
                        click.echo(f"  [ok] Changes saved to '{table_name}'")
                    else:
                        click.echo(
                            f"[warn] No primary key found for '{table_name}'"
                            f" — changes not saved."
                        )
                finally:
                    os.unlink(corrections_path)
        else:
            reviewer.show(df, title=f"{table_name} ({len(df)} rows)", pk_col=pk_col)

    finally:
        conn.close()


def table_commands(cli: click.Group) -> None:
    cli.add_command(table_cmd)
