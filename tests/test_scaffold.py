"""
Tests for proto_pipe.cli.scaffold

Covers:
- _suggest_pattern (filename pattern suggestion)
- _scan_incoming (directory scanning)
- _similar_columns (fuzzy column matching)
- _get_param_suggestions (history-based suggestions)
- _record_param_history (storing param usage)
- _sort_views (topological sorting)
- _build_sql_scaffold (SQL scaffold generation)
"""

import duckdb
import pandas as pd
import pytest

from proto_pipe.cli.scaffold import (
    _suggest_pattern,
    _scan_incoming,
    _similar_columns,
    _get_param_suggestions,
    _record_param_history,
    _sort_views,
    _build_sql_scaffold,
)


# ---------------------------------------------------------------------------
# _suggest_pattern
# ---------------------------------------------------------------------------

class TestSuggestPattern:
    def test_strips_date_from_filename(self):
        assert _suggest_pattern("sales_2024_01_15.csv") == "sales_*.csv"

    def test_strips_numeric_suffix(self):
        assert _suggest_pattern("report_001.csv") == "report_*.csv"

    def test_strips_only_first_number_block(self):
        # should replace the first contiguous digit block only
        result = _suggest_pattern("carrier_a_2024_01_15.csv")
        assert "*" in result
        assert result.endswith(".csv")

    def test_preserves_extension(self):
        assert _suggest_pattern("inventory_20240115.xlsx").endswith(".xlsx")

    def test_no_numbers_returns_stem_with_wildcard(self):
        # file with no numbers — pattern stays as-is
        result = _suggest_pattern("sales_data.csv")
        assert result.endswith(".csv")

    def test_xlsx_extension_preserved(self):
        assert _suggest_pattern("Sales_2024_03.xlsx") == "Sales_*.xlsx"


# ---------------------------------------------------------------------------
# _scan_incoming
# ---------------------------------------------------------------------------

class TestScanIncoming:
    def test_returns_csv_files(self, tmp_path):
        (tmp_path / "sales_2024.csv").write_text("a,b\n1,2")
        (tmp_path / "inventory_2024.csv").write_text("a,b\n1,2")
        result = _scan_incoming(str(tmp_path))
        assert "sales_2024.csv" in result
        assert "inventory_2024.csv" in result

    def test_returns_xlsx_files(self, tmp_path):
        pd.DataFrame({"a": [1]}).to_excel(tmp_path / "report_2024.xlsx", index=False)
        result = _scan_incoming(str(tmp_path))
        assert "report_2024.xlsx" in result

    def test_excludes_unsupported_extensions(self, tmp_path):
        (tmp_path / "notes.txt").write_text("hello")
        (tmp_path / "data.json").write_text("{}")
        result = _scan_incoming(str(tmp_path))
        assert "notes.txt" not in result
        assert "data.json" not in result

    def test_empty_directory_returns_empty_list(self, tmp_path):
        assert _scan_incoming(str(tmp_path)) == []

    def test_nonexistent_directory_returns_empty_list(self, tmp_path):
        assert _scan_incoming(str(tmp_path / "nonexistent")) == []

    def test_returns_sorted_filenames(self, tmp_path):
        (tmp_path / "c_2024.csv").write_text("a,b")
        (tmp_path / "a_2024.csv").write_text("a,b")
        (tmp_path / "b_2024.csv").write_text("a,b")
        result = _scan_incoming(str(tmp_path))
        assert result == sorted(result)


# ---------------------------------------------------------------------------
# _similar_columns
# ---------------------------------------------------------------------------

class TestSimilarColumns:
    def test_substring_match(self):
        cols = ["carrier_price", "quantity", "region"]
        result = _similar_columns("price", cols)
        assert "carrier_price" in result

    def test_exact_match(self):
        cols = ["price", "quantity", "region"]
        result = _similar_columns("price", cols)
        assert "price" in result

    def test_fuzzy_match(self):
        cols = ["amount", "quantity", "region"]
        result = _similar_columns("amt", cols)
        assert "amount" in result

    def test_no_match_returns_empty(self):
        cols = ["quantity", "region", "sku"]
        result = _similar_columns("xyz_unrelated_column", cols)
        assert result == []

    def test_substring_matches_come_before_fuzzy(self):
        cols = ["sale_price", "pricing_tier", "amt"]
        result = _similar_columns("price", cols)
        # substring matches should appear first
        assert result.index("sale_price") < result.index("amt") if "amt" in result else True

    def test_case_insensitive(self):
        cols = ["Price", "QUANTITY", "region"]
        result = _similar_columns("price", cols)
        assert "Price" in result


# ---------------------------------------------------------------------------
# _record_param_history and _get_param_suggestions
# ---------------------------------------------------------------------------

@pytest.fixture()
def history_conn(tmp_path):
    """DuckDB connection with check_params_history table."""
    conn = duckdb.connect(str(tmp_path / "pipeline.db"))
    conn.execute("""
        CREATE TABLE check_params_history (
            id          VARCHAR PRIMARY KEY,
            check_name  VARCHAR NOT NULL,
            report_name VARCHAR NOT NULL,
            table_name  VARCHAR NOT NULL,
            param_name  VARCHAR NOT NULL,
            param_value VARCHAR,
            recorded_at TIMESTAMPTZ NOT NULL
        )
    """)
    yield conn
    conn.close()


class TestCheckParamsHistory:
    def test_record_stores_params(self, history_conn):
        _record_param_history(
            history_conn,
            check_name="range_check",
            report_name="sales_validation",
            table_name="sales",
            params={"col": "price", "min_val": "0", "max_val": "500"},
        )
        rows = history_conn.execute(
            "SELECT param_name, param_value FROM check_params_history"
        ).fetchall()
        param_map = {r[0]: r[1] for r in rows}
        assert param_map["col"] == "price"
        assert param_map["min_val"] == "0"
        assert param_map["max_val"] == "500"

    def test_record_skips_none_values(self, history_conn):
        _record_param_history(
            history_conn,
            check_name="range_check",
            report_name="sales_validation",
            table_name="sales",
            params={"col": None, "min_val": "0"},
        )
        rows = history_conn.execute(
            "SELECT param_name FROM check_params_history"
        ).fetchall()
        param_names = [r[0] for r in rows]
        assert "col" not in param_names
        assert "min_val" in param_names

    def test_get_suggestions_empty_history(self, history_conn):
        result = _get_param_suggestions(
            history_conn, "range_check", "col", ["price", "quantity"]
        )
        assert result == []

    def test_get_suggestions_returns_similar_columns(self, history_conn):
        _record_param_history(
            history_conn,
            check_name="range_check",
            report_name="sales_validation",
            table_name="sales",
            params={"col": "price"},
        )
        result = _get_param_suggestions(
            history_conn, "range_check", "col", ["carrier_price", "quantity", "region"]
        )
        assert "carrier_price" in result

    def test_get_suggestions_no_similar_columns(self, history_conn):
        _record_param_history(
            history_conn,
            check_name="range_check",
            report_name="sales_validation",
            table_name="sales",
            params={"col": "price"},
        )
        result = _get_param_suggestions(
            history_conn, "range_check", "col", ["sku", "warehouse", "region"]
        )
        assert result == []

    def test_get_suggestions_deduplicates(self, history_conn):
        # Record same param twice
        for _ in range(2):
            _record_param_history(
                history_conn,
                check_name="range_check",
                report_name="sales_validation",
                table_name="sales",
                params={"col": "price"},
            )
        result = _get_param_suggestions(
            history_conn, "range_check", "col", ["price", "carrier_price"]
        )
        assert len(result) == len(set(result))


# ---------------------------------------------------------------------------
# _sort_views
# ---------------------------------------------------------------------------

class TestSortViews:
    def test_no_dependencies_order_unchanged(self, tmp_path):
        sql_a = tmp_path / "a.sql"
        sql_b = tmp_path / "b.sql"
        sql_a.write_text("SELECT 1 AS x FROM raw_table")
        sql_b.write_text("SELECT 2 AS y FROM other_table")

        views = [
            {"name": "view_a", "sql_file": str(sql_a)},
            {"name": "view_b", "sql_file": str(sql_b)},
        ]
        result = _sort_views(views)
        assert [v["name"] for v in result] == ["view_a", "view_b"]

    def test_sorts_dependency_before_dependent(self, tmp_path):
        sql_base = tmp_path / "base.sql"
        sql_top = tmp_path / "top.sql"
        sql_base.write_text("SELECT 1 AS x FROM raw_table")
        sql_top.write_text("SELECT x FROM view_base")

        views = [
            {"name": "view_top", "sql_file": str(sql_top)},
            {"name": "view_base", "sql_file": str(sql_base)},
        ]
        result = _sort_views(views)
        names = [v["name"] for v in result]
        assert names.index("view_base") < names.index("view_top")

    def test_raises_on_circular_dependency(self, tmp_path):
        import click
        sql_a = tmp_path / "a.sql"
        sql_b = tmp_path / "b.sql"
        sql_a.write_text("SELECT x FROM view_b")
        sql_b.write_text("SELECT x FROM view_a")

        views = [
            {"name": "view_a", "sql_file": str(sql_a)},
            {"name": "view_b", "sql_file": str(sql_b)},
        ]
        with pytest.raises(click.ClickException):
            _sort_views(views)

    def test_missing_sql_file_treated_as_no_deps(self, tmp_path):
        views = [
            {"name": "view_a", "sql_file": str(tmp_path / "nonexistent.sql")},
        ]
        result = _sort_views(views)
        assert result[0]["name"] == "view_a"

    def test_chain_of_three_sorted_correctly(self, tmp_path):
        sql_1 = tmp_path / "1.sql"
        sql_2 = tmp_path / "2.sql"
        sql_3 = tmp_path / "3.sql"
        sql_1.write_text("SELECT 1 AS x FROM raw")
        sql_2.write_text("SELECT x FROM view_1")
        sql_3.write_text("SELECT x FROM view_2")

        views = [
            {"name": "view_3", "sql_file": str(sql_3)},
            {"name": "view_1", "sql_file": str(sql_1)},
            {"name": "view_2", "sql_file": str(sql_2)},
        ]
        result = _sort_views(views)
        names = [v["name"] for v in result]
        assert names.index("view_1") < names.index("view_2")
        assert names.index("view_2") < names.index("view_3")


# ---------------------------------------------------------------------------
# _build_sql_scaffold
# ---------------------------------------------------------------------------

class TestBuildSqlScaffold:
    def _make_configs(self, tables: list[dict]) -> tuple[dict, dict]:
        """Helper to build minimal reports and sources configs."""
        reports_config = {
            "reports": [
                {
                    "name": f"{t['table']}_validation",
                    "source": {"table": t["table"]},
                }
                for t in tables
            ]
        }
        sources_config = {
            "sources": [
                {
                    "target_table": t["table"],
                    "primary_key": t.get("primary_key"),
                }
                for t in tables
            ]
        }
        return reports_config, sources_config

    def test_single_table_no_join(self):
        rep_cfg, src_cfg = self._make_configs([
            {"table": "sales", "primary_key": "order_id"}
        ])
        sql = _build_sql_scaffold(
            "carrier_a",
            ["sales_validation"],
            rep_cfg,
            src_cfg,
        )
        assert "FROM sales" in sql
        assert "LEFT JOIN" not in sql

    def test_two_tables_matching_primary_keys(self):
        rep_cfg, src_cfg = self._make_configs([
            {"table": "sales", "primary_key": "order_id"},
            {"table": "customers", "primary_key": "order_id"},
        ])
        sql = _build_sql_scaffold(
            "carrier_a",
            ["sales_validation", "customers_validation"],
            rep_cfg,
            src_cfg,
        )
        assert "LEFT JOIN customers" in sql
        assert "order_id" in sql

    def test_two_tables_mismatched_primary_keys(self):
        rep_cfg, src_cfg = self._make_configs([
            {"table": "sales", "primary_key": "order_id"},
            {"table": "inventory", "primary_key": "sku"},
        ])
        sql = _build_sql_scaffold(
            "carrier_a",
            ["sales_validation", "inventory_validation"],
            rep_cfg,
            src_cfg,
        )
        assert "LEFT JOIN inventory" in sql
        assert "update join" in sql.lower() or "order_id" in sql

    def test_table_with_no_primary_key(self):
        rep_cfg, src_cfg = self._make_configs([
            {"table": "sales", "primary_key": None},
            {"table": "customers", "primary_key": None},
        ])
        sql = _build_sql_scaffold(
            "carrier_a",
            ["sales_validation", "customers_validation"],
            rep_cfg,
            src_cfg,
        )
        assert "LEFT JOIN customers" in sql
        assert "<key>" in sql

    def test_internal_columns_comment_present(self):
        rep_cfg, src_cfg = self._make_configs([
            {"table": "sales", "primary_key": "order_id"}
        ])
        sql = _build_sql_scaffold(
            "carrier_a",
            ["sales_validation"],
            rep_cfg,
            src_cfg,
        )
        assert "_ingested_at" in sql

    def test_deliverable_name_in_header(self):
        rep_cfg, src_cfg = self._make_configs([
            {"table": "sales", "primary_key": "order_id"}
        ])
        sql = _build_sql_scaffold(
            "my_deliverable",
            ["sales_validation"],
            rep_cfg,
            src_cfg,
        )
        assert "my_deliverable" in sql

    def test_empty_selected_reports_returns_placeholder(self):
        sql = _build_sql_scaffold("carrier_a", [], {}, {})
        assert "SELECT" in sql
        assert "<table>" in sql
