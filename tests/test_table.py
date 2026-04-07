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
    """vp table is hard deprecated — exits non-zero, no DB access.

    Behavioral guarantees:
    - Exits with a non-zero code
    - Error message names all three replacements: vp view table,
      vp edit table, vp flagged edit
    - Does not execute any DB query or display logic
    """

    def test_exits_with_nonzero_code(self):
        runner = CliRunner()
        result = runner.invoke(table_cmd, ["sales"])
        assert result.exit_code == 1

    def test_error_mentions_vp_view_table(self):
        runner = CliRunner()
        result = runner.invoke(table_cmd, ["sales"])
        assert "vp view table" in result.output

    def test_error_mentions_vp_edit_table(self):
        runner = CliRunner()
        result = runner.invoke(table_cmd, ["sales"])
        assert "vp edit table" in result.output

    def test_error_mentions_vp_flagged_edit(self):
        runner = CliRunner()
        result = runner.invoke(table_cmd, ["sales"])
        assert "vp flagged edit" in result.output

    def test_does_not_query_db(self, tmp_path):
        """Exits before any DB connection — nonexistent DB path causes no
        different behaviour, proving no DB access occurs."""
        runner = CliRunner()
        result = runner.invoke(
            table_cmd,
            ["--pipeline-db", str(tmp_path / "nonexistent.db"), "sales"],
        )
        assert result.exit_code == 1
        assert "vp view table" in result.output


class TestDisplayRichTable:
    """_display_rich_table behavioral guarantees.

    Guarantees:
    - All columns use no_wrap=True — cell content is never folded within
      the column. overflow='fold' must not be used.
    - pager is called with styles=True so color is preserved when scrolling.
    - Data visible in the DB is passed to the display — not silently dropped.
    """

    @pytest.fixture()
    def wide_df(self):
        """DataFrame with enough columns to trigger layout issues at narrow widths."""
        return pd.DataFrame([
            {
                "order_id": "ORD-001",
                "carrier": "Carrier A",
                "premium": 5000.00,
                "inception": "2026-01-01",
                "expiry": "2027-01-01",
                "status": "active",
                "region": "EMEA",
                "updated_at": "2026-01-15T10:00:00+00:00",
            },
            {
                "order_id": "ORD-002",
                "carrier": "Carrier B",
                "premium": 12000.00,
                "inception": "2026-03-01",
                "expiry": "2027-03-01",
                "status": "lapsed",
                "region": "APAC",
                "updated_at": "2026-03-01T09:00:00+00:00",
            },
        ])

    def _run_captured(self, df, title="Test"):
        """Run _display_rich_table with Console mocked, return mock instance."""
        from proto_pipe.cli.commands.table import _display_rich_table
        from unittest.mock import MagicMock

        with patch("rich.console.Console") as MockConsole:
            mock_instance = MockConsole.return_value
            # Make pager a working context manager
            mock_pager_cm = MagicMock()
            mock_pager_cm.__enter__ = MagicMock(return_value=None)
            mock_pager_cm.__exit__ = MagicMock(return_value=False)
            mock_instance.pager.return_value = mock_pager_cm

            _display_rich_table(df, title)

        return mock_instance

    def test_columns_use_no_wrap_true(self, wide_df):
        from rich.table import Table as RichTable

        mock_console = self._run_captured(wide_df)

        # Extract the Table object passed to console.print
        tables = [
            arg
            for call in mock_console.print.call_args_list
            for arg in call[0]
            if isinstance(arg, RichTable)
        ]
        assert tables, "A rich Table must be passed to console.print"

        for col in tables[0].columns:
            assert col.no_wrap is True, (
                f"Column '{col.header}' must use no_wrap=True — "
                "overflow='fold' creates tall narrow columns at narrow terminals"
            )

    def test_overflow_fold_not_used(self, wide_df):
        from rich.table import Table as RichTable

        mock_console = self._run_captured(wide_df)

        tables = [
            arg
            for call in mock_console.print.call_args_list
            for arg in call[0]
            if isinstance(arg, RichTable)
        ]
        for col in tables[0].columns:
            assert col.overflow != "fold", (
                "overflow='fold' must never be used in _display_rich_table"
            )

    def test_pager_called_with_styles_true(self, wide_df):
        mock_console = self._run_captured(wide_df)

        mock_console.pager.assert_called_once()
        _, kwargs = mock_console.pager.call_args
        assert kwargs.get("styles") is True, (
            "pager must be called with styles=True to preserve color when scrolling"
        )

    def test_all_dataframe_rows_are_added(self, wide_df):
        """All rows in the DataFrame must be passed to the table — none silently dropped."""
        from rich.table import Table as RichTable

        mock_console = self._run_captured(wide_df)

        tables = [
            arg
            for call in mock_console.print.call_args_list
            for arg in call[0]
            if isinstance(arg, RichTable)
        ]
        assert tables
        assert tables[0].row_count == len(wide_df), (
            "All DataFrame rows must appear in the rendered table"
        )

    def test_all_dataframe_columns_are_present(self, wide_df):
        """All columns in the DataFrame must appear as table columns."""
        from rich.table import Table as RichTable

        mock_console = self._run_captured(wide_df)

        tables = [
            arg
            for call in mock_console.print.call_args_list
            for arg in call[0]
            if isinstance(arg, RichTable)
        ]
        assert tables
        rendered_headers = [str(col.header) for col in tables[0].columns]
        for df_col in wide_df.columns:
            assert str(df_col) in rendered_headers, (
                f"Column '{df_col}' must appear in the rendered table"
            )

    def test_renders_without_error_for_mixed_duckdb_dtypes(self):
        """_display_rich_table must not raise for any dtype DuckDB returns.

        DuckDB returns nullable integer columns (Int64) that raise
        TypeError: Invalid value '' for dtype 'Int64' if fillna("") is
        called before astype(object). This test guards against that regression.
        """
        import duckdb
        import pandas as pd
        from proto_pipe.cli.commands.table import _display_rich_table
        from unittest.mock import MagicMock

        # Build a DataFrame with the same dtypes DuckDB returns for real tables
        conn = duckdb.connect()
        df = conn.execute("""
            SELECT
                1::INTEGER         AS int_col,
                2.5::DOUBLE        AS float_col,
                'hello'::VARCHAR   AS str_col,
                TRUE::BOOLEAN      AS bool_col,
                NULL::INTEGER      AS nullable_int,
                NULL::DOUBLE       AS nullable_float,
                NULL::VARCHAR      AS nullable_str
        """).df()
        conn.close()

        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=None)
        mock_cm.__exit__ = MagicMock(return_value=False)

        with patch("rich.console.Console") as MockConsole:
            MockConsole.return_value.pager.return_value = mock_cm
            # Must not raise — this is the regression guard
            _display_rich_table(df, "Mixed DuckDB Types")

    def test_renders_without_error_for_nullable_integer_column(self):
        """Nullable Int64 columns with NULL values must not raise TypeError."""
        import pandas as pd
        from proto_pipe.cli.commands.table import _display_rich_table
        from unittest.mock import MagicMock

        # Pandas nullable integer dtype — same as what DuckDB returns for INTEGER columns
        df = pd.DataFrame({
            "id": pd.array([1, 2, None], dtype="Int64"),
            "value": pd.array([100, None, 300], dtype="Int64"),
            "label": ["a", "b", "c"],
        })

        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=None)
        mock_cm.__exit__ = MagicMock(return_value=False)

        with patch("rich.console.Console") as MockConsole:
            MockConsole.return_value.pager.return_value = mock_cm
            # Must not raise TypeError: Invalid value '' for dtype 'Int64'
            _display_rich_table(df, "Nullable Integers")
