
"""Tests for validation_pipeline.query

Covers:
- build_filter_clause (no filters, date filters, field filters, combined)
- query_table (unfiltered, date-filtered, field-filtered, CLI overrides)
- _merge_filters (override replaces same col, additive for new cols)
"""

import duckdb
import pandas as pd
import pytest

from src.reports.query import build_filter_clause, query_table, _merge_filters


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def sales_conn(populated_db):
    conn = duckdb.connect(populated_db)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# build_filter_clause
# ---------------------------------------------------------------------------

class TestBuildFilterClause:
    def test_no_filters_returns_empty(self):
        clause, params = build_filter_clause({})
        assert clause == ""
        assert params == []

    def test_date_from_only(self):
        clause, params = build_filter_clause(
            {"date_filters": [{"col": "order_date", "from": "2026-02-01"}]}
        )
        assert '"order_date" >= ?' in clause
        assert "2026-02-01" in params

    def test_date_to_only(self):
        clause, params = build_filter_clause(
            {"date_filters": [{"col": "order_date", "to": "2026-03-31"}]}
        )
        assert '"order_date" <= ?' in clause
        assert "2026-03-31" in params

    def test_date_from_and_to(self):
        clause, params = build_filter_clause(
            {
                "date_filters": [
                    {"col": "order_date", "from": "2026-01-01", "to": "2026-03-31"}
                ]
            }
        )
        assert '"order_date" >= ?' in clause
        assert '"order_date" <= ?' in clause
        assert len(params) == 2

    def test_field_filter(self):
        clause, params = build_filter_clause(
            {"field_filters": [{"col": "region", "values": ["EMEA", "APAC"]}]}
        )
        assert '"region" IN (?, ?)' in clause
        assert "EMEA" in params
        assert "APAC" in params

    def test_combined_filters(self):
        clause, params = build_filter_clause(
            {
                "date_filters": [{"col": "order_date", "from": "2026-01-01"}],
                "field_filters": [{"col": "region", "values": ["EMEA"]}],
            }
        )
        assert "WHERE" in clause
        assert "AND" in clause
        assert len(params) == 2

    def test_clause_starts_with_where(self):
        clause, _ = build_filter_clause(
            {"date_filters": [{"col": "order_date", "from": "2026-01-01"}]}
        )
        assert clause.startswith("WHERE")


# ---------------------------------------------------------------------------
# query_table
# ---------------------------------------------------------------------------

class TestQueryTable:
    def test_no_filters_returns_all_rows(self, sales_conn):
        df = query_table(sales_conn, "sales")
        assert len(df) == 3

    def test_date_from_filter(self, sales_conn):
        df = query_table(
            sales_conn,
            "sales",
            filters={"date_filters": [{"col": "order_date", "from": "2026-02-01"}]},
        )
        # ORD-001 (Jan) should be excluded
        assert len(df) == 2
        assert "ORD-001" not in df["order_id"].values

    def test_date_range_filter(self, sales_conn):
        df = query_table(
            sales_conn,
            "sales",
            filters={
                "date_filters": [
                    {"col": "order_date", "from": "2026-02-01", "to": "2026-02-28"}
                ]
            },
        )
        assert len(df) == 1
        assert df.iloc[0]["order_id"] == "ORD-002"

    def test_field_filter(self, sales_conn):
        df = query_table(
            sales_conn,
            "sales",
            filters={"field_filters": [{"col": "region", "values": ["EMEA"]}]},
        )
        assert all(df["region"] == "EMEA")

    def test_cli_override_replaces_config_date_filter(self, sales_conn):
        config_filters = {
            "date_filters": [{"col": "order_date", "from": "2026-01-01", "to": "2026-01-31"}]
        }
        cli_override = {
            "date_filters": [{"col": "order_date", "from": "2026-03-01"}]
        }
        df = query_table(sales_conn, "sales", filters=config_filters, cli_overrides=cli_override)
        # CLI replaces the config filter — should return March rows only
        assert len(df) == 1
        assert df.iloc[0]["order_id"] == "ORD-003"

    def test_cli_override_additive_for_new_col(self, sales_conn):
        config_filters = {
            "date_filters": [{"col": "order_date", "from": "2026-01-01"}]
        }
        cli_override = {
            "field_filters": [{"col": "region", "values": ["EMEA"]}]
        }
        df = query_table(sales_conn, "sales", filters=config_filters, cli_overrides=cli_override)
        assert all(df["region"] == "EMEA")
        # Jan EMEA row should still be present (date filter kept)
        assert "ORD-001" in df["order_id"].values

    def test_empty_result_is_dataframe(self, sales_conn):
        df = query_table(
            sales_conn,
            "sales",
            filters={"date_filters": [{"col": "order_date", "from": "2099-01-01"}]},
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


# ---------------------------------------------------------------------------
# _merge_filters
# ---------------------------------------------------------------------------

class TestMergeFilters:
    def test_override_replaces_same_col(self):
        base = {
            "date_filters": [{"col": "order_date", "from": "2026-01-01"}]
        }
        overrides = {
            "date_filters": [{"col": "order_date", "from": "2026-03-01"}]
        }
        _merge_filters(base, overrides)
        assert len(base["date_filters"]) == 1
        assert base["date_filters"][0]["from"] == "2026-03-01"

    def test_override_adds_new_col(self):
        base = {
            "date_filters": [{"col": "order_date", "from": "2026-01-01"}]
        }
        overrides = {
            "date_filters": [{"col": "updated_at", "from": "2026-02-01"}]
        }
        _merge_filters(base, overrides)
        cols = {f["col"] for f in base["date_filters"]}
        assert "order_date" in cols
        assert "updated_at" in cols

    def test_override_missing_filter_type_is_noop(self):
        base = {
            "date_filters": [{"col": "order_date", "from": "2026-01-01"}]
        }
        _merge_filters(base, {})
        assert len(base["date_filters"]) == 1
