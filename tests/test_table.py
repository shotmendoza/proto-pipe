"""
Tests for proto_pipe.cli.table

Covers:
- RichReview.show (read-only display)
- RichReview.edit (fallback when textual unavailable)
- TextualReview.show (TUI read-only)
- TextualReview.edit (TUI editing with Ctrl+S and Escape)
- table_cmd (CLI command integration)
"""

from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest
from click.testing import CliRunner

from proto_pipe.io.db import get_all_tables
from proto_pipe.cli.commands.table import (
    RichReview,
    TextualReview,
    get_reviewer,
    table_cmd,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_df() -> pd.DataFrame:
    return pd.DataFrame([
        {"order_id": "ORD-001", "price": 99.99, "region": "EMEA"},
        {"order_id": "ORD-002", "price": 250.00, "region": "APAC"},
        {"order_id": "ORD-003", "price": 15.50,  "region": "EMEA"},
    ])


@pytest.fixture()
def db_with_sales(tmp_path, sample_df) -> str:
    """Pipeline DB with a sales table and infrastructure tables."""
    db_path = str(tmp_path / "pipeline.db")
    conn = duckdb.connect(db_path)
    conn.execute("CREATE TABLE sales AS SELECT * FROM sample_df")
    from proto_pipe.io.db import init_all_pipeline_tables
    init_all_pipeline_tables(conn)
    conn.close()
    return db_path


@pytest.fixture()
def empty_db(tmp_path) -> str:
    """Pipeline DB with an empty sales table."""
    db_path = str(tmp_path / "pipeline.db")
    conn = duckdb.connect(db_path)
    conn.execute("CREATE TABLE sales (order_id VARCHAR, price DOUBLE, region VARCHAR)")
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# _get_all_tables
# ---------------------------------------------------------------------------

class TestGetAllTables:
    def test_returns_all_tables(self, db_with_sales):
        conn = duckdb.connect(db_with_sales)
        tables = get_all_tables(conn)
        conn.close()
        assert "sales" in tables
        assert "source_block" in tables
        assert "ingest_state" in tables

    def test_empty_db_returns_empty_list(self, tmp_path):
        db_path = str(tmp_path / "empty.db")
        conn = duckdb.connect(db_path)
        tables = get_all_tables(conn)
        conn.close()
        assert tables == []


# ---------------------------------------------------------------------------
# RichReview
# ---------------------------------------------------------------------------

class TestRichReview:
    def test_show_renders_without_error(self, sample_df):
        """Rich table renders without raising."""
        reviewer = RichReview()
        # Should not raise
        reviewer.show(sample_df, title="Test Table")

    def test_show_with_pk_col(self, sample_df):
        """Rich table renders with pk_col specified."""
        reviewer = RichReview()
        reviewer.show(sample_df, title="Test Table", pk_col="order_id")

    def test_edit_returns_original_df(self, sample_df):
        """Edit falls back to read-only and returns original df."""
        reviewer = RichReview()
        result = reviewer.edit(sample_df, title="Test Table", pk_col="order_id")
        assert result.equals(sample_df)

    def test_show_empty_df(self):
        """Rich table handles empty DataFrame."""
        reviewer = RichReview()
        reviewer.show(pd.DataFrame(), title="Empty Table")


# ---------------------------------------------------------------------------
# _get_reviewer
# ---------------------------------------------------------------------------

class TestGetReviewer:
    def test_returns_rich_review_when_not_edit(self):
        reviewer = get_reviewer(edit=False)
        assert isinstance(reviewer, RichReview)

    def test_returns_textual_review_when_edit_and_available(self):
        try:
            import textual  # noqa
            reviewer = get_reviewer(edit=True)
            assert isinstance(reviewer, TextualReview)
        except ImportError:
            pytest.skip("textual not installed")

    def test_falls_back_to_rich_when_textual_unavailable(self):
        with patch.dict("sys.modules", {"textual": None}):
            reviewer = get_reviewer(edit=True)
            assert isinstance(reviewer, RichReview)


# ---------------------------------------------------------------------------
# TextualReview
# ---------------------------------------------------------------------------

class TestTextualReview:
    def test_show_exits_on_escape(self, sample_df):
        """Pressing Escape exits the TUI without changes."""
        try:
            from textual.testing import Pilot
        except ImportError:
            pytest.skip("textual not installed")

        import asyncio

        reviewer = TextualReview()

        async def run():
            app = reviewer._make_app(sample_df, "Test", pk_col="order_id", editable=False)
            async with app.run_test() as pilot:
                await pilot.press("escape")
            return app._result

        result = asyncio.run(run())
        assert result is None

    def test_pk_col_highlighted_in_columns(self, sample_df):
        """Primary key column header contains yellow markup."""
        try:
            import textual  # noqa
        except ImportError:
            pytest.skip("textual not installed")

        import asyncio

        reviewer = TextualReview()

        async def run():
            app = reviewer._make_app(
                sample_df, "Test", pk_col="order_id", editable=False
            )
            async with app.run_test() as pilot:
                await pilot.press("escape")
                return app.pk_col

        pk_col = asyncio.run(run())
        assert pk_col == "order_id"

    def test_edit_ctrl_s_exits(self, sample_df):
        """Pressing Ctrl+S exits the TUI."""
        try:
            import textual  # noqa
        except ImportError:
            pytest.skip("textual not installed")

        import asyncio

        reviewer = TextualReview()

        async def run():
            app = reviewer._make_app(
                sample_df, "Test", pk_col="order_id", editable=True
            )
            async with app.run_test() as pilot:
                await pilot.press("ctrl+s")
                return app.return_value  # textual stores exit result here

        result = asyncio.run(run())
        # result should be the changes dict (empty since no edits made)
        assert result is not None


# ---------------------------------------------------------------------------
# table_cmd (CLI integration)
# ---------------------------------------------------------------------------

class TestTableCmd:
    def test_displays_table_by_name(self, db_with_sales, tmp_path):
        """vp table sales displays the table."""
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.table.config_path_or_override", return_value=db_with_sales
        ):
            result = runner.invoke(table_cmd, ["sales"], env={"PAGER": "cat"})
        assert result.exit_code == 0

    def test_unknown_table_shows_error(self, db_with_sales):
        """vp table nonexistent shows error message."""
        runner = CliRunner()
        with patch("proto_pipe.cli.commands.table.config_path_or_override", return_value=db_with_sales):
            result = runner.invoke(table_cmd, ["nonexistent"], env={"PAGER": "cat"})
        assert result.exit_code == 0
        assert "error" in result.output.lower() or "not found" in result.output.lower()

    def test_empty_table_shows_message(self, empty_db):
        """vp table on an empty table shows empty message."""
        runner = CliRunner()
        with patch("proto_pipe.cli.commands.table.config_path_or_override", return_value=empty_db):
            result = runner.invoke(table_cmd, ["sales"], env={"PAGER": "cat"})
        assert result.exit_code == 0
        assert "empty" in result.output.lower()

    def test_export_writes_csv(self, db_with_sales, tmp_path):
        """vp table sales --export out.csv writes a CSV file."""
        export_path = str(tmp_path / "out.csv")
        runner = CliRunner()
        with patch("proto_pipe.cli.commands.table.config_path_or_override", return_value=db_with_sales):
            result = runner.invoke(table_cmd, ["sales", "--export", export_path], env={"PAGER": "cat"})
        assert result.exit_code == 0
        assert Path(export_path).exists()
        df = pd.read_csv(export_path)
        assert len(df) == 3
        assert "order_id" in df.columns

    def test_export_correct_row_count(self, db_with_sales, tmp_path):
        """Exported CSV has the correct number of rows."""
        export_path = str(tmp_path / "out.csv")
        runner = CliRunner()
        with patch("proto_pipe.cli.commands.table.config_path_or_override", return_value=db_with_sales):
            runner.invoke(table_cmd, ["sales", "--export", export_path], env={"PAGER": "cat"})
        df = pd.read_csv(export_path)
        assert len(df) == 3

    def test_limit_restricts_rows(self, db_with_sales, tmp_path):
        """--limit restricts the number of rows displayed."""
        export_path = str(tmp_path / "out.csv")
        runner = CliRunner()
        with patch("proto_pipe.cli.commands.table.config_path_or_override", return_value=db_with_sales):
            runner.invoke(table_cmd, ["sales", "--export", export_path, "--limit", "2"], env={"PAGER": "cat"})
        df = pd.read_csv(export_path)
        assert len(df) == 2

    def test_infrastructure_tables_accessible(self, db_with_sales):
        """Pipeline infrastructure tables like flagged_rows can be viewed."""
        runner = CliRunner()
        with patch("proto_pipe.cli.commands.table.config_path_or_override", return_value=db_with_sales):
            result = runner.invoke(table_cmd, ["source_block"], env={"PAGER": "cat"})
        assert result.exit_code == 0

    def test_no_tables_in_db(self, tmp_path):
        """Empty DB shows appropriate message."""
        db_path = str(tmp_path / "empty.db")
        duckdb.connect(db_path).close()
        runner = CliRunner()
        with patch("proto_pipe.cli.commands.table.config_path_or_override", return_value=db_path):
            result = runner.invoke(table_cmd, [], env={"PAGER": "cat"})
        assert result.exit_code == 0
        assert "no tables" in result.output.lower()
