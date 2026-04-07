"""Tests for validation_pipeline.query

Covers:
- build_filter_clause (no filters, date filters, field filters, combined)
- query_table (unfiltered, date-filtered, field-filtered, CLI overrides)
- _merge_filters (override replaces same col, additive for new cols)
"""
import calendar
from datetime import date, timedelta

import duckdb
import pandas as pd
import pytest

from proto_pipe.pipelines.query import (
    build_filter_clause,
    query_table,
    _merge_filters,
    resolve_date_token,
    execute_sql_file,
    resolve_filter_dates,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def sales_conn(populated_db):
    conn = duckdb.connect(populated_db)
    yield conn
    conn.close()


def _today() -> date:
    return date.today()


def _make_conn(rows: list[dict] | None = None) -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection, optionally seeded with a sales table."""
    conn = duckdb.connect(":memory:")
    if rows is not None:
        df = pd.DataFrame(rows)
        conn.execute("CREATE TABLE sales AS SELECT * FROM df")
    return conn


# ---------------------------------------------------------------------------
# resolve_date_token — pass-through cases
# ---------------------------------------------------------------------------

class TestResolveDateTokenPassThrough:
    def test_hardcoded_date_unchanged(self):
        assert resolve_date_token("2026-01-01") == "2026-01-01"

    def test_arbitrary_string_unchanged(self):
        assert resolve_date_token("not_a_token") == "not_a_token"

    def test_non_string_int_unchanged(self):
        assert resolve_date_token(42) == 42

    def test_non_string_none_unchanged(self):
        assert resolve_date_token(None) is None

    def test_empty_string_unchanged(self):
        assert resolve_date_token("") == ""


# ---------------------------------------------------------------------------
# resolve_date_token — quarter tokens
# ---------------------------------------------------------------------------
class TestResolveDateTokenQuarters:
    def _quarter_start_month(self, d: date) -> int:
        return ((d.month - 1) // 3) * 3 + 1

    def test_start_of_quarter_is_day_1(self):
        result = date.fromisoformat(resolve_date_token("start_of_quarter"))
        assert result.day == 1

    def test_start_of_quarter_is_correct_month(self):
        t = _today()
        expected_month = self._quarter_start_month(t)
        result = date.fromisoformat(resolve_date_token("start_of_quarter"))
        assert result.month == expected_month

    def test_end_of_quarter_is_last_day_of_quarter(self):
        t = _today()
        q_end_month = ((t.month - 1) // 3) * 3 + 3
        last_day = calendar.monthrange(t.year, q_end_month)[1]
        expected = t.replace(month=q_end_month, day=last_day)
        result = date.fromisoformat(resolve_date_token("end_of_quarter"))
        assert result == expected

    def test_end_of_last_quarter_before_start_of_quarter(self):
        start_q = date.fromisoformat(resolve_date_token("start_of_quarter"))
        end_last_q = date.fromisoformat(resolve_date_token("end_of_last_quarter"))
        assert end_last_q < start_q

    def test_start_of_last_quarter_is_day_1(self):
        result = date.fromisoformat(resolve_date_token("start_of_last_quarter"))
        assert result.day == 1

    def test_start_of_last_quarter_before_end_of_last_quarter(self):
        start = date.fromisoformat(resolve_date_token("start_of_last_quarter"))
        end   = date.fromisoformat(resolve_date_token("end_of_last_quarter"))
        assert start <= end

    def test_quarter_tokens_return_iso_strings(self):
        for token in ("start_of_quarter", "end_of_quarter",
                      "start_of_last_quarter", "end_of_last_quarter"):
            result = resolve_date_token(token)
            # Should parse without raising
            date.fromisoformat(result)


# ---------------------------------------------------------------------------
# resolve_date_token — relative offsets
# ---------------------------------------------------------------------------

class TestResolveDateTokenOffsets:
    def test_today_minus_7d(self):
        expected = (_today() - timedelta(days=7)).isoformat()
        assert resolve_date_token("today-7d") == expected

    def test_today_minus_30d(self):
        expected = (_today() - timedelta(days=30)).isoformat()
        assert resolve_date_token("today-30d") == expected

    def test_today_plus_7d(self):
        expected = (_today() + timedelta(days=7)).isoformat()
        assert resolve_date_token("today+7d") == expected

    def test_today_minus_1d_is_yesterday(self):
        yesterday = _today() - timedelta(days=1)
        assert resolve_date_token("today-1d") == yesterday.isoformat()

    def test_today_plus_0d_is_today(self):
        # today+0d is a valid pattern; result should equal today
        assert resolve_date_token("today+0d") == _today().isoformat()

    def test_offset_uppercase_normalised(self):
        expected = (_today() - timedelta(days=7)).isoformat()
        assert resolve_date_token("TODAY-7D") == expected

    def test_invalid_offset_pattern_passthrough(self):
        # Missing the 'd' suffix — not a valid token, should pass through
        assert resolve_date_token("today-7") == "today-7"


# ---------------------------------------------------------------------------
# query_table — sql_file path
# ---------------------------------------------------------------------------

class TestQueryTableSqlFile:
    def test_sql_file_path_returns_correct_data(self, tmp_path):
        conn = _make_conn([
            {"order_id": 1, "price": 100.0, "region": "EMEA"},
            {"order_id": 2, "price": 200.0, "region": "APAC"},
        ])
        sql_file = tmp_path / "q.sql"
        sql_file.write_text('SELECT * FROM sales WHERE region = \'EMEA\'')
        df = query_table(conn, table=None, sql_file=str(sql_file))
        assert len(df) == 1
        assert df["region"].iloc[0] == "EMEA"

    def test_sql_file_ignores_filters(self, tmp_path):
        """Filters should be completely ignored when sql_file is set."""
        conn = _make_conn([
            {"order_id": 1, "price": 100.0, "region": "EMEA"},
            {"order_id": 2, "price": 200.0, "region": "APAC"},
        ])
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT * FROM sales")
        filters = {"field_filters": [{"col": "region", "values": ["EMEA"]}]}
        # Despite the filter, all rows should be returned because sql_file wins
        df = query_table(conn, table="sales", filters=filters, sql_file=str(sql_file))
        assert len(df) == 2

    def test_sql_file_ignores_cli_overrides(self, tmp_path):
        conn = _make_conn([
            {"order_id": 1, "price": 100.0, "region": "EMEA"},
            {"order_id": 2, "price": 200.0, "region": "APAC"},
        ])
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT * FROM sales")
        cli_overrides = {"field_filters": [{"col": "region", "values": ["EMEA"]}]}
        df = query_table(conn, table="sales", cli_overrides=cli_overrides, sql_file=str(sql_file))
        assert len(df) == 2

    def test_sql_file_missing_raises(self, tmp_path):
        conn = duckdb.connect(":memory:")
        with pytest.raises(FileNotFoundError):
            query_table(conn, table=None, sql_file=str(tmp_path / "missing.sql"))

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


class TestBuildFilterClauseNew:
    def test_empty_filters_returns_empty(self):
        clause, params = build_filter_clause({})
        assert clause == ""
        assert params == []

    def test_date_from_only(self):
        filters = {"date_filters": [{"col": "order_date", "from": "2026-01-01"}]}
        clause, params = build_filter_clause(filters)
        assert '"order_date" >= ?' in clause
        assert params == ["2026-01-01"]

    def test_date_to_only(self):
        filters = {"date_filters": [{"col": "order_date", "to": "2026-03-31"}]}
        clause, params = build_filter_clause(filters)
        assert '"order_date" <= ?' in clause
        assert params == ["2026-03-31"]

    def test_date_from_and_to(self):
        filters = {"date_filters": [{"col": "order_date", "from": "2026-01-01", "to": "2026-03-31"}]}
        clause, params = build_filter_clause(filters)
        assert '"order_date" >= ?' in clause
        assert '"order_date" <= ?' in clause
        assert params == ["2026-01-01", "2026-03-31"]

    def test_field_filter_single_value(self):
        filters = {"field_filters": [{"col": "region", "values": ["EMEA"]}]}
        clause, params = build_filter_clause(filters)
        assert '"region" IN (?)' in clause
        assert params == ["EMEA"]

    def test_field_filter_multiple_values(self):
        filters = {"field_filters": [{"col": "region", "values": ["EMEA", "APAC"]}]}
        clause, params = build_filter_clause(filters)
        assert '"region" IN (?, ?)' in clause
        assert params == ["EMEA", "APAC"]

    def test_combined_date_and_field_filters(self):
        filters = {
            "date_filters":  [{"col": "order_date", "from": "2026-01-01"}],
            "field_filters": [{"col": "region", "values": ["EMEA"]}],
        }
        clause, params = build_filter_clause(filters)
        assert "WHERE" in clause
        assert "AND" in clause
        assert len(params) == 2

    def test_token_resolved_in_clause(self):
        filters = {"date_filters": [{"col": "order_date", "to": "end_of_last_month"}]}
        clause, params = build_filter_clause(filters)
        expected_date = (_today().replace(day=1) - timedelta(days=1)).isoformat()
        assert params == [expected_date]

    def test_multiple_date_columns(self):
        filters = {
            "date_filters": [
                {"col": "order_date", "to": "end_of_last_month"},
                {"col": "updated_at", "to": "end_of_last_month"},
            ]
        }
        clause, params = build_filter_clause(filters)
        assert '"order_date" <= ?' in clause
        assert '"updated_at" <= ?' in clause
        assert len(params) == 2

    def test_clause_starts_with_where(self):
        filters = {"date_filters": [{"col": "d", "from": "2026-01-01"}]}
        clause, _ = build_filter_clause(filters)
        assert clause.startswith("WHERE")

    def test_no_date_filters_key_still_works(self):
        filters = {"field_filters": [{"col": "status", "values": ["active"]}]}
        clause, params = build_filter_clause(filters)
        assert "status" in clause
        assert params == ["active"]


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


# ---------------------------------------------------------------------------
# resolve_date_token — simple tokens
# ---------------------------------------------------------------------------
class TestResolveDateTokenSimple:
    def test_today(self):
        assert resolve_date_token("today") == _today().isoformat()

    def test_today_uppercase_normalised(self):
        # tokens are case-insensitive
        assert resolve_date_token("TODAY") == _today().isoformat()

    def test_today_mixed_case_normalised(self):
        assert resolve_date_token("Today") == _today().isoformat()

    def test_start_of_month(self):
        expected = _today().replace(day=1).isoformat()
        assert resolve_date_token("start_of_month") == expected

    def test_end_of_month(self):
        t = _today()
        last_day = calendar.monthrange(t.year, t.month)[1]
        expected = t.replace(day=last_day).isoformat()
        assert resolve_date_token("end_of_month") == expected

    def test_start_of_last_month(self):
        t = _today()
        first_of_this = t.replace(day=1)
        last_month_end = first_of_this - timedelta(days=1)
        expected = last_month_end.replace(day=1).isoformat()
        assert resolve_date_token("start_of_last_month") == expected

    def test_end_of_last_month(self):
        expected = (_today().replace(day=1) - timedelta(days=1)).isoformat()
        assert resolve_date_token("end_of_last_month") == expected

    def test_start_of_month_is_day_1(self):
        result = date.fromisoformat(resolve_date_token("start_of_month"))
        assert result.day == 1

    def test_end_of_month_is_valid_date(self):
        result = date.fromisoformat(resolve_date_token("end_of_month"))
        assert result.month == _today().month

    def test_start_of_last_month_is_day_1(self):
        result = date.fromisoformat(resolve_date_token("start_of_last_month"))
        assert result.day == 1

    def test_end_of_last_month_is_last_day(self):
        result = date.fromisoformat(resolve_date_token("end_of_last_month"))
        t = _today()
        first_of_this = t.replace(day=1)
        last_month_end = first_of_this - timedelta(days=1)
        assert result == last_month_end


# ---------------------------------------------------------------------------
# execute_sql_file
# ---------------------------------------------------------------------------
class TestExecuteSqlFile:
    def test_executes_valid_sql_file(self, tmp_path):
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT 1 AS val, 'hello' AS msg")
        conn = duckdb.connect(":memory:")
        df = execute_sql_file(conn, str(sql_file))
        assert len(df) == 1
        assert df["val"].iloc[0] == 1
        assert df["msg"].iloc[0] == "hello"

    def test_executes_join_query(self, tmp_path):
        conn = _make_conn([
            {"id": 1, "name": "Alice", "dept_id": 10},
            {"id": 2, "name": "Bob",   "dept_id": 20},
        ])
        conn.execute("CREATE TABLE depts AS SELECT * FROM (VALUES (10, 'Eng'), (20, 'Sales')) t(dept_id, dept_name)")
        sql_file = tmp_path / "joined.sql"
        sql_file.write_text("""
            SELECT e.name, d.dept_name
            FROM sales e
            JOIN depts d ON e.dept_id = d.dept_id
            ORDER BY e.id
        """)
        df = execute_sql_file(conn, str(sql_file))
        assert list(df["name"]) == ["Alice", "Bob"]
        assert list(df["dept_name"]) == ["Eng", "Sales"]

    def test_executes_sql_with_duckdb_date_functions(self, tmp_path):
        sql_file = tmp_path / "dates.sql"
        sql_file.write_text("SELECT current_date AS today")
        conn = duckdb.connect(":memory:")
        df = execute_sql_file(conn, str(sql_file))
        assert len(df) == 1
        assert "today" in df.columns

    def test_raises_file_not_found(self, tmp_path):
        conn = duckdb.connect(":memory:")
        with pytest.raises(FileNotFoundError, match="SQL file not found"):
            execute_sql_file(conn, str(tmp_path / "nonexistent.sql"))

    def test_raises_on_empty_file(self, tmp_path):
        sql_file = tmp_path / "empty.sql"
        sql_file.write_text("")
        conn = duckdb.connect(":memory:")
        with pytest.raises(ValueError, match="SQL file is empty"):
            execute_sql_file(conn, str(sql_file))

    def test_raises_on_whitespace_only_file(self, tmp_path):
        sql_file = tmp_path / "whitespace.sql"
        sql_file.write_text("   \n\t  ")
        conn = duckdb.connect(":memory:")
        with pytest.raises(ValueError, match="SQL file is empty"):
            execute_sql_file(conn, str(sql_file))

    def test_returns_dataframe(self, tmp_path):
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT 1 AS x")
        conn = duckdb.connect(":memory:")
        result = execute_sql_file(conn, str(sql_file))
        assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# query_table — filter path
# ---------------------------------------------------------------------------
class TestQueryTableFilters:
    def _seed_conn(self):
        return _make_conn([
            {"order_id": 1, "price": 50.0,  "region": "EMEA", "order_date": "2026-01-15"},
            {"order_id": 2, "price": 150.0, "region": "APAC", "order_date": "2026-02-20"},
            {"order_id": 3, "price": 250.0, "region": "EMEA", "order_date": "2026-03-10"},
        ])

    def test_no_filters_returns_all_rows(self):
        conn = self._seed_conn()
        df = query_table(conn, "sales")
        assert len(df) == 3

    def test_empty_filters_returns_all_rows(self):
        conn = self._seed_conn()
        df = query_table(conn, "sales", filters={})
        assert len(df) == 3

    def test_field_filter_applied(self):
        conn = self._seed_conn()
        filters = {"field_filters": [{"col": "region", "values": ["EMEA"]}]}
        df = query_table(conn, "sales", filters=filters)
        assert len(df) == 2
        assert all(df["region"] == "EMEA")

    def test_date_from_filter_applied(self):
        conn = self._seed_conn()
        filters = {"date_filters": [{"col": "order_date", "from": "2026-02-01"}]}
        df = query_table(conn, "sales", filters=filters)
        assert len(df) == 2
        assert all(df["order_date"] >= "2026-02-01")

    def test_date_to_filter_applied(self):
        conn = self._seed_conn()
        filters = {"date_filters": [{"col": "order_date", "to": "2026-02-28"}]}
        df = query_table(conn, "sales", filters=filters)
        assert len(df) == 2

    def test_combined_date_and_field_filters(self):
        conn = self._seed_conn()
        filters = {
            "date_filters":  [{"col": "order_date", "from": "2026-01-01", "to": "2026-02-28"}],
            "field_filters": [{"col": "region", "values": ["EMEA"]}],
        }
        df = query_table(conn, "sales", filters=filters)
        assert len(df) == 1
        assert df["order_id"].iloc[0] == 1

    def test_cli_override_replaces_config_date_filter(self):
        conn = self._seed_conn()
        filters = {"date_filters": [{"col": "order_date", "from": "2026-01-01", "to": "2026-01-31"}]}
        cli_overrides = {"date_filters": [{"col": "order_date", "from": "2026-03-01"}]}
        df = query_table(conn, "sales", filters=filters, cli_overrides=cli_overrides)
        # CLI override replaces the order_date filter — should return only March row
        assert len(df) == 1
        assert df["order_id"].iloc[0] == 3

    def test_no_matching_rows_returns_empty_dataframe(self):
        conn = self._seed_conn()
        filters = {"field_filters": [{"col": "region", "values": ["LATAM"]}]}
        df = query_table(conn, "sales", filters=filters)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_returns_dataframe(self):
        conn = self._seed_conn()
        result = query_table(conn, "sales")
        assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# resolve_filter_dates
# ---------------------------------------------------------------------------

class TestResolveFilterDates:
    def test_resolves_from_token(self):
        filters = {"date_filters": [{"col": "order_date", "from": "start_of_month"}]}
        result = resolve_filter_dates(filters)
        expected = _today().replace(day=1).isoformat()
        assert result["date_filters"][0]["from"] == expected

    def test_resolves_to_token(self):
        filters = {"date_filters": [{"col": "order_date", "to": "end_of_last_month"}]}
        result = resolve_filter_dates(filters)
        expected = (_today().replace(day=1) - timedelta(days=1)).isoformat()
        assert result["date_filters"][0]["to"] == expected

    def test_hardcoded_date_unchanged(self):
        filters = {"date_filters": [{"col": "order_date", "from": "2026-01-01"}]}
        result = resolve_filter_dates(filters)
        assert result["date_filters"][0]["from"] == "2026-01-01"

    def test_missing_from_key_not_added(self):
        filters = {"date_filters": [{"col": "order_date", "to": "today"}]}
        result = resolve_filter_dates(filters)
        assert "from" not in result["date_filters"][0]

    def test_missing_to_key_not_added(self):
        filters = {"date_filters": [{"col": "order_date", "from": "today"}]}
        result = resolve_filter_dates(filters)
        assert "to" not in result["date_filters"][0]

    def test_field_filters_passed_through_unchanged(self):
        filters = {
            "date_filters":  [],
            "field_filters": [{"col": "region", "values": ["EMEA"]}],
        }
        result = resolve_filter_dates(filters)
        assert result["field_filters"] == [{"col": "region", "values": ["EMEA"]}]

    def test_multiple_date_filters_all_resolved(self):
        filters = {
            "date_filters": [
                {"col": "order_date", "to": "end_of_last_month"},
                {"col": "updated_at", "to": "end_of_last_month"},
            ]
        }
        result = resolve_filter_dates(filters)
        expected = (_today().replace(day=1) - timedelta(days=1)).isoformat()
        assert result["date_filters"][0]["to"] == expected
        assert result["date_filters"][1]["to"] == expected

    def test_does_not_mutate_original(self):
        filters = {"date_filters": [{"col": "order_date", "from": "start_of_month"}]}
        original_value = filters["date_filters"][0]["from"]
        resolve_filter_dates(filters)
        assert filters["date_filters"][0]["from"] == original_value

    def test_empty_filters_returns_empty_date_filters(self):
        result = resolve_filter_dates({})
        assert result["date_filters"] == []
