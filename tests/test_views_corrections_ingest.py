"""
Tests for:
  - views.py — _load_sql, create_views, refresh_views, load_views_config
  - corrections.py — export_flagged, import_corrections
  - ingest.py — _already_ingested (deduplication behaviour)

Run with:
  pytest tests/test_views_corrections_ingest.py -v
"""

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from proto_pipe.io.db import already_ingested, flag_id_for, init_ingest_state
from proto_pipe.reports.corrections import import_corrections, export_flagged
from proto_pipe.io.db import init_all_pipeline_tables, init_ingest_state
from proto_pipe.reports.views import (
    create_views,
    refresh_views,
    load_views_config,
    _load_sql,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _validation_flag_row(
    conn, table_name: str, pk_value: str, check_name: str, reason: str
) -> str:
    """Insert a validation_block entry and return its id."""
    import hashlib
    key = f"test_report:{check_name}:{pk_value}"
    flag_id = hashlib.md5(key.encode()).hexdigest()
    conn.execute("""
        INSERT INTO validation_block
            (id, table_name, report_name, check_name, pk_value, reason, flagged_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO NOTHING
    """, [flag_id, table_name, "test_report", check_name,
          pk_value, reason, datetime.now(timezone.utc)])
    return flag_id

@pytest.fixture
def conn():
    """Fresh in-memory DuckDB connection for each test."""
    return duckdb.connect(":memory:")


@pytest.fixture
def conn_with_sales(conn):
    """Connection seeded with a sales table and flagged_rows table."""
    conn.execute("""
        CREATE TABLE sales (
            order_id  VARCHAR,
            price     DOUBLE,
            region    VARCHAR,
            status    VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO sales VALUES
            ('ORD-001', 100.0, 'EMEA',  'active'),
            ('ORD-002', -50.0, 'APAC',  'active'),
            ('ORD-003', 200.0, 'EMEA',  'inactive'),
            ('ORD-004', 0.0,   'LATAM', 'active')
    """)
    # Bootstrap all pipeline tables including source_block (was flagged_rows)
    init_all_pipeline_tables(conn)
    return conn


@pytest.fixture
def conn_with_ingest_log(conn):
    """Connection with ingest_state table bootstrapped."""
    init_ingest_state(conn)
    return conn


@pytest.fixture
def conn_with_sales_and_validation(conn_with_sales):
    """conn_with_sales extended with the validation_block table."""
    init_all_pipeline_tables(conn_with_sales)
    return conn_with_sales


def _write_sql_file(tmp_path: Path, filename: str, sql: str) -> str:
    """Write a SQL file and return its path as a string."""
    p = tmp_path / filename
    p.write_text(sql)
    return str(p)


def _write_views_config(tmp_path: Path, views: list[dict]) -> str:
    """Write a views_config.yaml and return its path."""
    from ruamel.yaml import YAML
    import io
    yaml = YAML()
    stream = io.StringIO()

    p = tmp_path / "views_config.yaml"
    yaml.dump({"views": views}, stream)
    p.write_text(stream.getvalue())
    return str(p)


def _flag_row(conn, table_name: str, pk_value: str, reason: str) -> str:
    """Insert a source_block entry using md5(pk_value) as the id and return it."""
    flag_id = flag_id_for(pk_value)
    conn.execute("""
        INSERT INTO source_block (id, table_name, check_name, pk_value, reason, flagged_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO NOTHING
    """, [flag_id, table_name, "test_check", pk_value, reason, datetime.now(timezone.utc)])
    return flag_id

# ===========================================================================
# views.py — _load_sql
# ===========================================================================

class TestLoadSql:
    def test_reads_sql_file(self, tmp_path):
        sql_file = _write_sql_file(tmp_path, "q.sql", "SELECT 1 AS x")
        assert _load_sql(sql_file) == "SELECT 1 AS x"

    def test_strips_whitespace(self, tmp_path):
        sql_file = _write_sql_file(tmp_path, "q.sql", "  \n  SELECT 1  \n  ")
        assert _load_sql(sql_file) == "SELECT 1"

    def test_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="View SQL file not found"):
            _load_sql(str(tmp_path / "missing.sql"))

    def test_raises_on_empty_file(self, tmp_path):
        sql_file = _write_sql_file(tmp_path, "empty.sql", "")
        with pytest.raises(ValueError, match="View SQL file is empty"):
            _load_sql(sql_file)

    def test_raises_on_whitespace_only(self, tmp_path):
        sql_file = _write_sql_file(tmp_path, "ws.sql", "   \n\t  ")
        with pytest.raises(ValueError, match="View SQL file is empty"):
            _load_sql(sql_file)


# ===========================================================================
# views.py — create_views
# ===========================================================================

class TestCreateViews:
    def test_creates_single_view(self, conn, tmp_path):
        sql_file = _write_sql_file(tmp_path, "v.sql", "SELECT 42 AS answer")
        views = [{"name": "my_view", "sql_file": sql_file}]
        created = create_views(conn, views)
        assert created == ["my_view"]

    def test_view_is_queryable_after_creation(self, conn, tmp_path):
        sql_file = _write_sql_file(tmp_path, "v.sql", "SELECT 42 AS answer")
        create_views(conn, [{"name": "my_view", "sql_file": sql_file}])
        result = conn.execute("SELECT answer FROM my_view").fetchone()
        assert result[0] == 42

    def test_view_reflects_underlying_table(self, conn_with_sales, tmp_path):
        sql_file = _write_sql_file(
            tmp_path, "v.sql",
            "SELECT order_id, price FROM sales WHERE price > 0"
        )
        create_views(conn_with_sales, [{"name": "positive_sales", "sql_file": sql_file}])
        df = conn_with_sales.execute("SELECT * FROM positive_sales").df()
        assert len(df) == 2   # ORD-002 (price=-50) and ORD-004 (price=0) filtered out
        assert all(df["price"] > 0)

    def test_creates_multiple_views_in_order(self, conn, tmp_path):
        sql1 = _write_sql_file(tmp_path, "v1.sql", "SELECT 1 AS n")
        sql2 = _write_sql_file(tmp_path, "v2.sql", "SELECT n + 1 AS m FROM view_one")
        views = [
            {"name": "view_one", "sql_file": sql1},
            {"name": "view_two", "sql_file": sql2},
        ]
        created = create_views(conn, views)
        assert created == ["view_one", "view_two"]
        result = conn.execute("SELECT m FROM view_two").fetchone()
        assert result[0] == 2

    def test_skip_existing_view_when_replace_false(self, conn, tmp_path):
        sql_file = _write_sql_file(tmp_path, "v.sql", "SELECT 1 AS x")
        views = [{"name": "my_view", "sql_file": sql_file}]
        create_views(conn, views, replace=False)
        # Second call should not raise — IF NOT EXISTS skips it
        create_views(conn, views, replace=False)
        result = conn.execute("SELECT x FROM my_view").fetchone()
        assert result[0] == 1

    def test_replace_true_updates_view_definition(self, conn, tmp_path):
        sql_v1 = _write_sql_file(tmp_path, "v1.sql", "SELECT 1 AS x")
        sql_v2 = _write_sql_file(tmp_path, "v2.sql", "SELECT 99 AS x")
        views_v1 = [{"name": "my_view", "sql_file": sql_v1}]
        views_v2 = [{"name": "my_view", "sql_file": sql_v2}]

        create_views(conn, views_v1, replace=False)
        assert conn.execute("SELECT x FROM my_view").fetchone()[0] == 1

        create_views(conn, views_v2, replace=True)
        assert conn.execute("SELECT x FROM my_view").fetchone()[0] == 99

    def test_returns_list_of_created_names(self, conn, tmp_path):
        sql1 = _write_sql_file(tmp_path, "a.sql", "SELECT 1 AS x")
        sql2 = _write_sql_file(tmp_path, "b.sql", "SELECT 2 AS x")
        views = [
            {"name": "alpha", "sql_file": sql1},
            {"name": "beta",  "sql_file": sql2},
        ]
        result = create_views(conn, views)
        assert result == ["alpha", "beta"]

    def test_raises_on_missing_sql_file(self, conn, tmp_path):
        views = [{"name": "broken", "sql_file": str(tmp_path / "nope.sql")}]
        with pytest.raises(FileNotFoundError):
            create_views(conn, views)

    def test_empty_views_list_returns_empty(self, conn):
        result = create_views(conn, [])
        assert result == []


# ===========================================================================
# views.py — refresh_views
# ===========================================================================

class TestRefreshViews:
    def test_refresh_recreates_view(self, conn, tmp_path):
        sql_v1 = _write_sql_file(tmp_path, "v1.sql", "SELECT 1 AS val")
        sql_v2 = _write_sql_file(tmp_path, "v2.sql", "SELECT 2 AS val")

        create_views(conn, [{"name": "v", "sql_file": sql_v1}])
        assert conn.execute("SELECT val FROM v").fetchone()[0] == 1

        refresh_views(conn, [{"name": "v", "sql_file": sql_v2}])
        assert conn.execute("SELECT val FROM v").fetchone()[0] == 2

    def test_refresh_view_that_references_another(self, conn, tmp_path):
        sql_base = _write_sql_file(tmp_path, "base.sql", "SELECT 10 AS n")
        sql_top  = _write_sql_file(tmp_path, "top.sql",  "SELECT n * 2 AS m FROM base_view")
        views = [
            {"name": "base_view", "sql_file": sql_base},
            {"name": "top_view",  "sql_file": sql_top},
        ]
        create_views(conn, views)
        assert conn.execute("SELECT m FROM top_view").fetchone()[0] == 20

        # Refresh with updated base
        sql_base2 = _write_sql_file(tmp_path, "base2.sql", "SELECT 20 AS n")
        refresh_views(conn, [
            {"name": "base_view", "sql_file": sql_base2},
            {"name": "top_view",  "sql_file": sql_top},
        ])
        assert conn.execute("SELECT m FROM top_view").fetchone()[0] == 40

    def test_refresh_returns_list_of_names(self, conn, tmp_path):
        sql = _write_sql_file(tmp_path, "v.sql", "SELECT 1 AS x")
        views = [{"name": "v", "sql_file": sql}]
        create_views(conn, views)
        result = refresh_views(conn, views)
        assert result == ["v"]


# ===========================================================================
# views.py — load_views_config
# ===========================================================================

class TestLoadViewsConfig:
    def test_loads_views_from_yaml(self, tmp_path):
        cfg = _write_views_config(tmp_path, [
            {"name": "v1", "sql_file": "config/sql/views/v1.sql"},
            {"name": "v2", "sql_file": "config/sql/views/v2.sql"},
        ])
        result = load_views_config(cfg)
        assert len(result) == 2
        assert result[0]["name"] == "v1"
        assert result[1]["name"] == "v2"

    def test_returns_empty_list_if_file_missing(self, tmp_path):
        result = load_views_config(str(tmp_path / "nonexistent.yaml"))
        assert result == []

    def test_returns_empty_list_if_no_views_key(self, tmp_path):
        p = tmp_path / "views_config.yaml"
        p.write_text("other_key: []\n")
        result = load_views_config(str(p))
        assert result == []

    def test_returns_empty_list_for_empty_file(self, tmp_path):
        p = tmp_path / "views_config.yaml"
        p.write_text("")
        result = load_views_config(str(p))
        assert result == []

    def test_preserves_order(self, tmp_path):
        cfg = _write_views_config(tmp_path, [
            {"name": "first",  "sql_file": "a.sql"},
            {"name": "second", "sql_file": "b.sql"},
            {"name": "third",  "sql_file": "c.sql"},
        ])
        result = load_views_config(cfg)
        assert [v["name"] for v in result] == ["first", "second", "third"]


# ===========================================================================
# corrections.py — export_flagged
# ===========================================================================

class TestExportFlagged:
    def test_exports_flagged_rows_to_csv(self, conn_with_sales, tmp_path):
        _flag_row(conn_with_sales, "sales", "ORD-002", "price out of range")
        output = str(tmp_path / "flagged.csv")
        count = export_flagged(conn_with_sales, "sales", output, "order_id")
        assert count == 1
        assert Path(output).exists()

    def test_exported_csv_contains_source_columns(self, conn_with_sales, tmp_path):
        _flag_row(conn_with_sales, "sales", "ORD-002", "negative price")
        output = str(tmp_path / "flagged.csv")
        export_flagged(conn_with_sales, "sales", output, "order_id")
        df = pd.read_csv(output)
        assert "order_id" in df.columns
        assert "price" in df.columns
        assert "region" in df.columns

    def test_exported_csv_contains_annotation_columns(self, conn_with_sales, tmp_path):
        _flag_row(conn_with_sales, "sales", "ORD-002", "negative price")
        output = str(tmp_path / "flagged.csv")
        export_flagged(conn_with_sales, "sales", output, "order_id")
        df = pd.read_csv(output)
        assert "_flag_reason" in df.columns
        assert "_flag_id" in df.columns

    def test_annotation_columns_are_first(self, conn_with_sales, tmp_path):
        _flag_row(conn_with_sales, "sales", "ORD-002", "negative price")
        output = str(tmp_path / "flagged.csv")
        export_flagged(conn_with_sales, "sales", output, "order_id")
        df = pd.read_csv(output)
        assert df.columns[0] == "_flag_id"
        assert df.columns[1] == "_flag_reason"

    def test_flag_reason_matches(self, conn_with_sales, tmp_path):
        _flag_row(conn_with_sales, "sales", "ORD-002", "negative price")
        output = str(tmp_path / "flagged.csv")
        export_flagged(conn_with_sales, "sales", output, "order_id")
        df = pd.read_csv(output)
        assert df["_flag_reason"].iloc[0] == "negative price"

    def test_exports_multiple_flagged_rows(self, conn_with_sales, tmp_path):
        _flag_row(conn_with_sales, "sales", "ORD-002", "negative price")
        _flag_row(conn_with_sales, "sales", "ORD-004", "zero price")
        output = str(tmp_path / "flagged.csv")
        count = export_flagged(conn_with_sales, "sales", output, "order_id")
        assert count == 2

    def test_raises_if_no_flagged_rows(self, conn_with_sales, tmp_path):
        output = str(tmp_path / "flagged.csv")
        with pytest.raises(ValueError, match="No flagged rows found"):
            export_flagged(conn_with_sales, "sales", output, "order_id")

    def test_raises_if_table_is_empty(self, conn, tmp_path):
        conn.execute("CREATE TABLE sales (order_id VARCHAR, price DOUBLE)")
        conn.execute("""
            CREATE TABLE flagged_rows (
                id         VARCHAR PRIMARY KEY,
                table_name VARCHAR NOT NULL,
                check_name VARCHAR,
                reason     VARCHAR,
                flagged_at TIMESTAMPTZ NOT NULL
            )
        """)
        _flag_row(conn, "sales", "ORD-EMPTY", "test")
        output = str(tmp_path / "flagged.csv")
        with pytest.raises(ValueError, match="No source rows matched"):
            export_flagged(conn, "sales", output, "order_id")

    def test_only_exports_rows_for_specified_table(self, conn_with_sales, tmp_path):
        conn_with_sales.execute("CREATE TABLE orders (order_id VARCHAR, total DOUBLE)")
        conn_with_sales.execute("INSERT INTO orders VALUES ('X1', 5.0)")
        _flag_row(conn_with_sales, "sales",  "ORD-002", "bad price")
        _flag_row(conn_with_sales, "orders", "X1",      "bad total")
        output = str(tmp_path / "flagged.csv")
        count = export_flagged(conn_with_sales, "sales", output, "order_id")
        df = pd.read_csv(output)
        assert count == 1
        assert len(df) == 1

    def test_creates_output_directory_if_missing(self, conn_with_sales, tmp_path):
        _flag_row(conn_with_sales, "sales", "ORD-001", "reason")
        output = str(tmp_path / "subdir" / "nested" / "flagged.csv")
        export_flagged(conn_with_sales, "sales", output, "order_id")
        assert Path(output).exists()

    def test_returns_row_count(self, conn_with_sales, tmp_path):
        _flag_row(conn_with_sales, "sales", "ORD-001", "r1")
        _flag_row(conn_with_sales, "sales", "ORD-003", "r2")
        output = str(tmp_path / "f.csv")
        count = export_flagged(conn_with_sales, "sales", output, "order_id")
        assert count == 2


# ===========================================================================
# corrections.py — import_corrections
# ===========================================================================


class TestImportCorrections:
    def _make_corrections_csv(
        self, tmp_path, rows: list[dict], filename="corrections.csv"
    ) -> str:
        df = pd.DataFrame(rows)
        p = tmp_path / filename
        df.to_csv(p, index=False)
        return str(p)

    def test_updates_row_by_primary_key(self, conn_with_sales, tmp_path):
        path = self._make_corrections_csv(
            tmp_path,
            [
                {
                    "order_id": "ORD-002",
                    "price": 75.0,
                    "region": "APAC",
                    "status": "active",
                }
            ],
        )
        import_corrections(conn_with_sales, "sales", path, "order_id")
        result = conn_with_sales.execute(
            "SELECT price FROM sales WHERE order_id = 'ORD-002'"
        ).fetchone()
        assert result[0] == 75.0

    def test_only_updates_specified_rows(self, conn_with_sales, tmp_path):
        path = self._make_corrections_csv(
            tmp_path,
            [
                {
                    "order_id": "ORD-002",
                    "price": 75.0,
                    "region": "APAC",
                    "status": "active",
                }
            ],
        )
        import_corrections(conn_with_sales, "sales", path, "order_id")
        # ORD-001 should be untouched
        result = conn_with_sales.execute(
            "SELECT price FROM sales WHERE order_id = 'ORD-001'"
        ).fetchone()
        assert result[0] == 100.0

    def test_strips_annotation_columns_before_update(self, conn_with_sales, tmp_path):
        path = self._make_corrections_csv(
            tmp_path,
            [
                {
                    "_flag_id": "some-uuid",
                    "_flag_reason": "negative price",
                    "order_id": "ORD-002",
                    "price": 75.0,
                    "region": "APAC",
                    "status": "active",
                }
            ],
        )
        # Should not raise even though _flag_id/_flag_reason are not table columns
        result = import_corrections(conn_with_sales, "sales", path, "order_id")
        assert result["updated"] == 1

    def test_clears_flagged_rows_by_flag_id(self, conn_with_sales, tmp_path):
        flag_id = _flag_row(conn_with_sales, "sales", "ORD-002", "negative price")
        path = self._make_corrections_csv(
            tmp_path,
            [
                {
                    "_flag_id": flag_id,
                    "_flag_reason": "negative price",
                    "order_id": "ORD-002",
                    "price": 75.0,
                    "region": "APAC",
                    "status": "active",
                }
            ],
        )
        result = import_corrections(conn_with_sales, "sales", path, "order_id")
        assert result["flagged_cleared"] == 1
        remaining = conn_with_sales.execute(
            "SELECT count(*) FROM source_block"
        ).fetchone()[0]
        assert remaining == 0

    def test_returns_correct_updated_count(self, conn_with_sales, tmp_path):
        path = self._make_corrections_csv(
            tmp_path,
            [
                {
                    "order_id": "ORD-001",
                    "price": 110.0,
                    "region": "EMEA",
                    "status": "active",
                },
                {
                    "order_id": "ORD-002",
                    "price": 75.0,
                    "region": "APAC",
                    "status": "active",
                },
            ],
        )
        result = import_corrections(conn_with_sales, "sales", path, "order_id")
        assert result["updated"] == 2

    def test_returns_zero_flagged_cleared_when_no_flag_id_col(
        self, conn_with_sales, tmp_path
    ):
        # Corrections file without _flag_id — no flags to clear
        path = self._make_corrections_csv(
            tmp_path,
            [
                {
                    "order_id": "ORD-002",
                    "price": 75.0,
                    "region": "APAC",
                    "status": "active",
                }
            ],
        )
        result = import_corrections(conn_with_sales, "sales", path, "order_id")
        assert result["flagged_cleared"] == 0

    def test_raises_file_not_found(self, conn_with_sales, tmp_path):
        with pytest.raises(FileNotFoundError, match="Corrections file not found"):
            import_corrections(
                conn_with_sales, "sales", str(tmp_path / "missing.csv"), "order_id"
            )

    def test_raises_if_primary_key_missing_from_file(self, conn_with_sales, tmp_path):
        path = self._make_corrections_csv(
            tmp_path, [{"price": 75.0, "region": "APAC"}]  # order_id missing
        )
        with pytest.raises(ValueError, match="Primary key column 'order_id' not found"):
            import_corrections(conn_with_sales, "sales", path, "order_id")

    def test_raises_if_no_updatable_columns(self, conn_with_sales, tmp_path):
        # File only has the primary key — nothing to update
        path = self._make_corrections_csv(tmp_path, [{"order_id": "ORD-002"}])
        with pytest.raises(ValueError, match="No updatable columns found"):
            import_corrections(conn_with_sales, "sales", path, "order_id")

    def test_ignores_file_columns_not_in_table(self, conn_with_sales, tmp_path):
        # Extra column in the file that doesn't exist in the table
        path = self._make_corrections_csv(
            tmp_path,
            [
                {
                    "order_id": "ORD-002",
                    "price": 75.0,
                    "region": "APAC",
                    "status": "active",
                    "nonexistent_col": "ignored",
                }
            ],
        )
        # Should not raise — nonexistent_col is silently skipped
        result = import_corrections(conn_with_sales, "sales", path, "order_id")
        assert result["updated"] == 1

    def test_partial_corrections_dont_affect_unflagged_rows(
        self, conn_with_sales, tmp_path
    ):
        flag_id = _flag_row(conn_with_sales, "sales", "ORD-002", "bad price")
        path = self._make_corrections_csv(
            tmp_path,
            [
                {
                    "_flag_id": flag_id,
                    "_flag_reason": "bad price",
                    "order_id": "ORD-002",
                    "price": 75.0,
                    "region": "APAC",
                    "status": "active",
                }
            ],
        )
        import_corrections(conn_with_sales, "sales", path, "order_id")
        # ORD-003 untouched
        result = conn_with_sales.execute(
            "SELECT price FROM sales WHERE order_id = 'ORD-003'"
        ).fetchone()
        assert result[0] == 200.0

    def test_returns_validation_cleared_zero_when_no_validation_flags_table(
        self, conn_with_sales, tmp_path
    ):
        # conn_with_sales has flagged_rows but not validation_flags —
        # import_corrections should not raise, validation_cleared should be 0.
        flag_id = _flag_row(conn_with_sales, "sales", "ORD-002", "bad price")
        path = self._make_corrections_csv(
            tmp_path,
            [
                {
                    "_flag_id": flag_id,
                    "_flag_reason": "bad price",
                    "order_id": "ORD-002",
                    "price": 75.0,
                    "region": "APAC",
                    "status": "active",
                }
            ],
        )
        result = import_corrections(conn_with_sales, "sales", path, "order_id")
        assert result["flagged_cleared"] == 1
        # validation_flags table doesn't exist in this fixture — expect 0 or key absent
        assert result.get("validation_cleared", 0) == 0

    def test_clears_validation_flags_by_flag_id(
        self, conn_with_sales_and_validation, tmp_path
    ):
        conn = conn_with_sales_and_validation
        # Write a validation flag for ORD-002
        val_flag_id = _validation_flag_row(
            conn, "sales", "ORD-002", "price_check", "price negative"
        )

        path = self._make_corrections_csv(
            tmp_path,
            [
                {
                    "_flag_id": val_flag_id,
                    "_flag_reason": "price negative",
                    "order_id": "ORD-002",
                    "price": 75.0,
                    "region": "APAC",
                    "status": "active",
                }
            ],
        )
        result = import_corrections(conn, "sales", path, "order_id")

        assert result["updated"] == 1
        assert result["validation_cleared"] == 1
        assert result["flagged_cleared"] == 0
        remaining = conn.execute("SELECT count(*) FROM validation_block").fetchone()[0]
        assert remaining == 0

    def test_clears_only_matching_validation_flags(
        self, conn_with_sales_and_validation, tmp_path
    ):
        conn = conn_with_sales_and_validation
        # Flag two rows; only correct one of them
        val_flag_id = _validation_flag_row(
            conn, "sales", "ORD-002", "price_check", "bad"
        )
        _validation_flag_row(conn, "sales", "ORD-004", "price_check", "bad")

        path = self._make_corrections_csv(
            tmp_path,
            [
                {
                    "_flag_id": val_flag_id,
                    "_flag_reason": "bad",
                    "order_id": "ORD-002",
                    "price": 75.0,
                    "region": "APAC",
                    "status": "active",
                }
            ],
        )
        result = import_corrections(conn, "sales", path, "order_id")

        assert result["validation_cleared"] == 1
        remaining = conn.execute("SELECT count(*) FROM validation_block").fetchone()[0]
        assert remaining == 1  # ORD-004 still flagged

    def test_clears_flagged_rows_and_validation_flags_independently(
        self, conn_with_sales_and_validation, tmp_path
    ):
        conn = conn_with_sales_and_validation
        # ORD-001 has an ingest conflict; ORD-002 has a validation flag
        ingest_flag_id = _flag_row(conn, "sales", "ORD-001", "ingest conflict")
        val_flag_id = _validation_flag_row(
            conn, "sales", "ORD-002", "price_check", "bad"
        )

        path = self._make_corrections_csv(
            tmp_path,
            [
                {
                    "_flag_id": ingest_flag_id,
                    "_flag_reason": "ingest conflict",
                    "order_id": "ORD-001",
                    "price": 100.0,
                    "region": "EMEA",
                    "status": "active",
                },
                {
                    "_flag_id": val_flag_id,
                    "_flag_reason": "bad",
                    "order_id": "ORD-002",
                    "price": 75.0,
                    "region": "APAC",
                    "status": "active",
                },
            ],
        )
        result = import_corrections(conn, "sales", path, "order_id")

        assert result["updated"] == 2
        assert result["flagged_cleared"] == 1
        assert result["validation_cleared"] == 1
        assert conn.execute("SELECT count(*) FROM source_block").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM validation_block").fetchone()[0] == 0


# ===========================================================================
# ingest.py — _already_ingested
# ===========================================================================

class TestAlreadyIngested:
    def test_returns_false_for_unknown_file(self, conn_with_ingest_log):
        assert already_ingested(conn_with_ingest_log, "sales_jan.csv") is False

    def test_returns_true_after_ok_log(self, conn_with_ingest_log):
        log_ingest(conn_with_ingest_log, "sales_jan.csv", "sales", "ok", rows=10)
        assert already_ingested(conn_with_ingest_log, "sales_jan.csv") is True

    def test_returns_false_after_failed_log(self, conn_with_ingest_log):
        log_ingest(conn_with_ingest_log, "sales_jan.csv", "sales", "failed",
                   message="empty file")
        assert already_ingested(conn_with_ingest_log, "sales_jan.csv") is False

    def test_returns_false_after_skipped_log(self, conn_with_ingest_log):
        log_ingest(conn_with_ingest_log, "sales_jan.csv", "sales", "skipped",
                   message="no match")
        assert already_ingested(conn_with_ingest_log, "sales_jan.csv") is False

    def test_returns_true_even_if_also_has_prior_failure(self, conn_with_ingest_log):
        # File failed once, then succeeded — should be considered ingested
        log_ingest(conn_with_ingest_log, "sales_jan.csv", "sales", "failed",
                   message="bad file")
        log_ingest(conn_with_ingest_log, "sales_jan.csv", "sales", "ok", rows=10)
        assert already_ingested(conn_with_ingest_log, "sales_jan.csv") is True

    def test_different_filenames_are_independent(self, conn_with_ingest_log):
        log_ingest(conn_with_ingest_log, "sales_jan.csv", "sales", "ok", rows=10)
        assert already_ingested(conn_with_ingest_log, "sales_feb.csv") is False

    def test_filename_match_is_exact(self, conn_with_ingest_log):
        log_ingest(conn_with_ingest_log, "sales_jan.csv", "sales", "ok", rows=10)
        assert already_ingested(conn_with_ingest_log, "sales_jan") is False
        assert already_ingested(conn_with_ingest_log, "SALES_JAN.CSV") is False


# ===========================================================================
# Integration — export then import round-trip
# ===========================================================================

class TestCorrectionRoundTrip:
    def test_full_round_trip(self, conn_with_sales, tmp_path):
        """Export flagged rows, simulate user fix, import corrections, verify state."""
        # Flag row at index 1 (ORD-002, price=-50.0)
        _flag_row(conn_with_sales, "sales", "ORD-002", "negative price")

        # Export
        output = str(tmp_path / "flagged_sales.csv")
        count = export_flagged(conn_with_sales, "sales", output, "order_id")
        assert count == 1

        # Simulate user fixing the price in the CSV
        df = pd.read_csv(output)
        assert df["price"].iloc[0] == -50.0
        df.loc[0, "price"] = 75.0
        df.to_csv(output, index=False)

        # Import corrections
        result = import_corrections(conn_with_sales, "sales", output, "order_id")
        assert result["updated"] == 1
        assert result["flagged_cleared"] == 1

        # Verify the price was corrected in the table
        price = conn_with_sales.execute(
            "SELECT price FROM sales WHERE order_id = 'ORD-002'"
        ).fetchone()[0]
        assert price == 75.0

        # Verify flagged_rows is now empty
        remaining = conn_with_sales.execute(
            "SELECT count(*) FROM source_block"
        ).fetchone()[0]
        assert remaining == 0

    def test_view_reflects_corrected_data(self, conn_with_sales, tmp_path):
        """After correcting a row, a view built on that table reflects the fix."""
        # Create a view that filters to positive prices
        sql_file = _write_sql_file(
            tmp_path, "positive.sql",
            "SELECT order_id, price FROM sales WHERE price > 0"
        )
        create_views(conn_with_sales, [{"name": "positive_sales", "sql_file": sql_file}])

        # Before correction: ORD-002 (price=-50) should not appear in view
        df_before = conn_with_sales.execute("SELECT * FROM positive_sales").df()
        assert "ORD-002" not in df_before["order_id"].values

        # Flag and correct ORD-002
        output = str(tmp_path / "flagged.csv")
        _flag_row(conn_with_sales, "sales", "ORD-002", "negative price")
        export_flagged(conn_with_sales, "sales", output, "order_id")
        df = pd.read_csv(output)
        df.loc[0, "price"] = 75.0
        df.to_csv(output, index=False)
        import_corrections(conn_with_sales, "sales", output, "order_id")

        # After correction: ORD-002 should now appear in view (views are not materialised)
        df_after = conn_with_sales.execute("SELECT * FROM positive_sales").df()
        assert "ORD-002" in df_after["order_id"].values
