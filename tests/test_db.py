"""Tests for upsert_via_staging — UPDATE-only staging pattern.

Behavioral guarantees:
- Matching rows are updated
- Non-matching PKs are silently skipped (no INSERT)
- Empty DataFrame returns 0
- DataFrame missing pk_col returns 0
- Only non-PK columns are updated (PK itself unchanged)
- Multiple rows updated in single call
- Extra columns not in target table are silently ignored
"""
import duckdb
import pandas as pd
import pytest

from proto_pipe.io.db import upsert_via_staging


@pytest.fixture()
def conn(tmp_path):
    c = duckdb.connect(str(tmp_path / "test.db"))
    c.execute("""
        CREATE TABLE target (
            pk  VARCHAR PRIMARY KEY,
            val INTEGER,
            name VARCHAR
        )
    """)
    c.execute("""
        INSERT INTO target VALUES
            ('A', 1, 'alpha'),
            ('B', 2, 'beta'),
            ('C', 3, 'gamma')
    """)
    yield c
    c.close()


class TestUpsertViaStaging:
    def test_updates_matching_row(self, conn):
        df = pd.DataFrame({"pk": ["A"], "val": [99], "name": ["updated"]})
        count = upsert_via_staging(conn, "target", df, "pk")
        assert count == 1
        row = conn.execute("SELECT val, name FROM target WHERE pk = 'A'").fetchone()
        assert row == (99, "updated")

    def test_nonmatching_pk_silently_skipped(self, conn):
        df = pd.DataFrame({"pk": ["Z"], "val": [99], "name": ["new"]})
        upsert_via_staging(conn, "target", df, "pk")
        # No new row inserted
        total = conn.execute("SELECT count(*) FROM target").fetchone()[0]
        assert total == 3

    def test_empty_dataframe_returns_zero(self, conn):
        df = pd.DataFrame(columns=["pk", "val", "name"])
        assert upsert_via_staging(conn, "target", df, "pk") == 0

    def test_missing_pk_col_returns_zero(self, conn):
        df = pd.DataFrame({"val": [99], "name": ["x"]})
        assert upsert_via_staging(conn, "target", df, "pk") == 0

    def test_pk_column_value_unchanged(self, conn):
        df = pd.DataFrame({"pk": ["A"], "val": [99], "name": ["x"]})
        upsert_via_staging(conn, "target", df, "pk")
        pks = conn.execute("SELECT pk FROM target ORDER BY pk").df()["pk"].tolist()
        assert pks == ["A", "B", "C"]

    def test_multiple_rows_updated(self, conn):
        df = pd.DataFrame({
            "pk": ["A", "C"],
            "val": [10, 30],
            "name": ["a2", "c2"],
        })
        count = upsert_via_staging(conn, "target", df, "pk")
        assert count == 2
        a = conn.execute("SELECT val FROM target WHERE pk = 'A'").fetchone()[0]
        c = conn.execute("SELECT val FROM target WHERE pk = 'C'").fetchone()[0]
        assert a == 10
        assert c == 30
        # B unchanged
        b = conn.execute("SELECT val FROM target WHERE pk = 'B'").fetchone()[0]
        assert b == 2

    def test_extra_columns_silently_ignored(self, conn):
        df = pd.DataFrame({"pk": ["A"], "val": [50], "nonexistent": ["junk"]})
        upsert_via_staging(conn, "target", df, "pk")
        row = conn.execute("SELECT val FROM target WHERE pk = 'A'").fetchone()[0]
        assert row == 50
