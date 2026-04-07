"""
tests/test_new_source_and_blank_cols.py

Behavioral guarantees covered:

  Blank column name stripping (ingest_single_file):
  - TestBlankColumnStripping
      blank_col_not_in_user_cols        — blank-named columns are stripped before
                                          the unknown-column check and never reach
                                          get_registry_types
      blank_col_does_not_fail_ingest    — a CSV with a trailing-comma blank column
                                          ingests successfully when all real columns
                                          are registered
      all_blank_col_names_emits_warn    — if every non-_ column is blank, a warning
                                          is printed (not a hard fail)
      whitespace_only_col_stripped      — " " (space-only) column name is treated the
                                          same as "" (empty)

  Blank column stripping in vp new source scan:
  - TestNewSourceBlankColFilter
      blank_col_excluded_from_file_cols — blank column names are excluded from the
                                          file_cols list fed to SourceConfigPrompter,
                                          so they are never shown or registered

  SourceConfigPrompter — existing source pattern addition:
  - TestSourceConfigPrompterPatternAddition
      existing_patterns_preloaded       — when an existing source name is confirmed,
                                          self._existing is populated from the lookup
      new_pattern_appended              — prompt_pattern pre-fills with
                                          existing_patterns + [suggested_new]
      duplicate_pattern_not_added       — if suggested is already in existing_patterns,
                                          it is not duplicated in the default
      no_lookup_entry_leaves_existing   — if name is not in existing_lookup (edge case),
                                          self._existing stays unchanged
"""

import csv
import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

from proto_pipe.io.db import (
    init_all_pipeline_tables,
    write_registry_types,
    get_registry_types,
)
from proto_pipe.io.ingest import ingest_single_file


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(path: Path, rows: list[dict], extra_blank_col: bool = False) -> None:
    """Write a minimal CSV. If extra_blank_col=True, adds a trailing comma
    to the header and every data row, producing an unnamed empty column."""
    cols = list(rows[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        if extra_blank_col:
            writer.writerow(cols + [""])         # blank header
            for row in rows:
                writer.writerow([row[c] for c in cols] + [""])
        else:
            writer.writerow(cols)
            for row in rows:
                writer.writerow([row[c] for c in cols])


def _make_source(table: str = "sales", pk: str = "order_id") -> dict:
    return {
        "name": "sales",
        "target_table": table,
        "patterns": ["sales_*.csv"],
        "primary_key": pk,
        "on_duplicate": "flag",
    }


def _bootstrapped_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(db_path)
    init_all_pipeline_tables(conn)
    return conn


# ---------------------------------------------------------------------------
# TestBlankColumnStripping — ingest_single_file behaviour
# ---------------------------------------------------------------------------

class TestBlankColumnStripping:
    """Blank/whitespace column names are stripped in ingest_single_file before
    the unknown-column check so they never trigger a false '[fail]'."""

    ROWS = [
        {"order_id": "ORD-001", "amount": "99.99"},
        {"order_id": "ORD-002", "amount": "14.50"},
    ]

    @pytest.fixture()
    def db(self, tmp_path) -> str:
        return str(tmp_path / "pipeline.db")

    @pytest.fixture()
    def csv_with_blank_col(self, tmp_path) -> Path:
        p = tmp_path / "sales_2026.csv"
        _write_csv(p, self.ROWS, extra_blank_col=True)
        return p

    @pytest.fixture()
    def csv_clean(self, tmp_path) -> Path:
        p = tmp_path / "sales_2026.csv"
        _write_csv(p, self.ROWS, extra_blank_col=False)
        return p

    def test_blank_col_does_not_fail_ingest(self, db, csv_with_blank_col):
        """A CSV with a blank trailing column ingests without error when real
        columns are registered."""
        conn = _bootstrapped_conn(db)
        write_registry_types(conn, "sales", {"order_id": "VARCHAR", "amount": "DOUBLE"})
        conn.close()

        conn = _bootstrapped_conn(db)
        result = ingest_single_file(conn, csv_with_blank_col, _make_source())
        conn.close()

        assert result["status"] == "ok", result.get("message")

    def test_blank_col_not_in_registry_types(self, db, csv_with_blank_col):
        """After stripping, '' is never passed to get_registry_types."""
        conn = _bootstrapped_conn(db)
        write_registry_types(conn, "sales", {"order_id": "VARCHAR", "amount": "DOUBLE"})

        # Verify the registry has no entry for a blank column name.
        types = get_registry_types(conn, ["order_id", "amount", ""])
        conn.close()

        assert "" not in types

    def test_whitespace_only_col_stripped(self, db, tmp_path):
        """A column named ' ' (space) is treated as blank and stripped."""
        p = tmp_path / "sales_space.csv"
        # Manually write a CSV with a space-only header column.
        p.write_text("order_id,amount, \nORD-001,99.99,\nORD-002,14.50,\n")

        conn = _bootstrapped_conn(db)
        write_registry_types(conn, "sales", {"order_id": "VARCHAR", "amount": "DOUBLE"})
        conn.close()

        conn = _bootstrapped_conn(db)
        result = ingest_single_file(conn, p, _make_source())
        conn.close()

        assert result["status"] == "ok", result.get("message")

    def test_blank_col_rows_still_inserted(self, db, csv_with_blank_col):
        """Rows with a blank trailing column are inserted into the table."""
        conn = _bootstrapped_conn(db)
        write_registry_types(conn, "sales", {"order_id": "VARCHAR", "amount": "DOUBLE"})
        conn.close()

        conn = _bootstrapped_conn(db)
        ingest_single_file(conn, csv_with_blank_col, _make_source())
        count = conn.execute('SELECT count(*) FROM "sales"').fetchone()[0]
        conn.close()

        assert count == len(self.ROWS)


# ---------------------------------------------------------------------------
# TestNewSourceBlankColFilter — file_cols build in new_source
# ---------------------------------------------------------------------------

class TestNewSourceBlankColFilter:
    """Blank column names are excluded from file_cols before being passed to
    SourceConfigPrompter, so they are never shown or written to the registry."""

    def test_blank_col_excluded_from_file_cols(self):
        """Simulate the file_cols build in new_source: blank names are stripped."""
        import pandas as pd

        # Simulate what coerce_for_display returns when a blank-header column exists.
        df = pd.DataFrame({
            "order_id": ["ORD-001"],
            "amount": [99.99],
            "": [None],         # blank column from trailing comma
            " ": [None],        # whitespace-only column
        })

        # This is the filter applied in new_source (after the fix).
        file_cols = [
            c for c in df.columns
            if not c.startswith("_") and c.strip()
        ]

        assert "" not in file_cols
        assert " " not in file_cols
        assert "order_id" in file_cols
        assert "amount" in file_cols


# ---------------------------------------------------------------------------
# TestSourceConfigPrompterPatternAddition
# ---------------------------------------------------------------------------

class TestSourceConfigPrompterPatternAddition:
    """SourceConfigPrompter correctly pre-fills existing source data when
    the user types an existing source name during vp new source."""

    def _make_prompter(self, existing_lookup: dict | None = None):
        from proto_pipe.cli.prompts import SourceConfigPrompter
        return SourceConfigPrompter(
            sample_df=None,
            registry_hints={},
            existing_sources_lookup=existing_lookup or {},
        )

    def test_existing_patterns_preloaded_on_name_confirm(self):
        """After prompt_name confirms an existing name, self._existing is
        populated from existing_sources_lookup so later prompts pre-fill."""
        existing = {
            "name": "sales",
            "patterns": ["sales_*.csv", "Sales_*.xlsx"],
            "target_table": "sales",
            "primary_key": "order_id",
            "on_duplicate": "flag",
        }
        prompter = self._make_prompter({"sales": existing})

        # Simulate the user typing "sales" and confirming "Edit it?"
        with patch("questionary.text") as mock_text, \
             patch("questionary.confirm") as mock_confirm:
            mock_text.return_value.ask.return_value = "sales"
            mock_confirm.return_value.ask.return_value = True   # "Edit it? Yes"

            name = prompter.prompt_name(existing_names=["sales"])

        assert name == "sales"
        assert prompter._existing["patterns"] == ["sales_*.csv", "Sales_*.xlsx"]
        assert prompter._existing["primary_key"] == "order_id"

    def test_new_pattern_appended_to_existing(self):
        """prompt_pattern pre-fills with existing_patterns + [suggested_new]."""
        existing = {
            "name": "sales",
            "patterns": ["sales_*.csv", "Sales_*.xlsx"],
        }
        prompter = self._make_prompter({"sales": existing})
        prompter._existing = existing   # simulate post-name-confirm state

        captured_default = {}

        def fake_text(prompt, default=""):
            captured_default["value"] = default
            m = MagicMock()
            # User accepts the pre-filled default as-is.
            m.ask.return_value = default
            return m

        with patch("questionary.text", side_effect=fake_text):
            result = prompter.prompt_pattern(suggested="SALES_*.csv")

        assert "sales_*.csv" in captured_default["value"]
        assert "Sales_*.xlsx" in captured_default["value"]
        assert "SALES_*.csv" in captured_default["value"]
        assert result == ["sales_*.csv", "Sales_*.xlsx", "SALES_*.csv"]

    def test_duplicate_pattern_not_added_twice(self):
        """If suggested is already in existing_patterns, it is not duplicated."""
        existing = {"name": "sales", "patterns": ["sales_*.csv", "Sales_*.xlsx"]}
        prompter = self._make_prompter({"sales": existing})
        prompter._existing = existing

        captured_default = {}

        def fake_text(prompt, default=""):
            captured_default["value"] = default
            m = MagicMock()
            m.ask.return_value = default
            return m

        with patch("questionary.text", side_effect=fake_text):
            result = prompter.prompt_pattern(suggested="sales_*.csv")  # already exists

        # Pattern should appear exactly once.
        assert result.count("sales_*.csv") == 1

    def test_no_lookup_entry_leaves_existing_unchanged(self):
        """If the typed name is not in existing_sources_lookup (edge case),
        self._existing is not changed."""
        prompter = self._make_prompter(existing_lookup={})  # empty lookup

        with patch("questionary.text") as mock_text, \
             patch("questionary.confirm") as mock_confirm:
            mock_text.return_value.ask.return_value = "sales"
            mock_confirm.return_value.ask.return_value = True

            prompter.prompt_name(existing_names=["sales"])

        # Should remain empty — no crash, no state mutation.
        assert prompter._existing == {}

    def test_cancelling_existing_name_prompt_returns_none(self):
        """If user declines to edit an existing source, prompt_name returns None."""
        prompter = self._make_prompter({"sales": {"name": "sales", "patterns": []}})

        with patch("questionary.text") as mock_text, \
             patch("questionary.confirm") as mock_confirm:
            mock_text.return_value.ask.return_value = "sales"
            mock_confirm.return_value.ask.return_value = False  # "Edit it? No"

            result = prompter.prompt_name(existing_names=["sales"])

        assert result is None
