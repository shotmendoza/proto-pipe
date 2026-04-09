"""Tests for shared flag table functions — get_raw_flags, clear_flags.

These are the parameterized helpers that replaced the paired
_get_raw_source_block / _get_raw_validation_block and inline
DELETE ... WHERE id IN blocks in cli/flagged.py.

Behavioral guarantees:
- get_raw_flags returns standard _flag_* aliases regardless of flag table
- get_raw_flags filters by arbitrary column=value pairs
- get_raw_flags respects limit
- get_raw_flags returns empty DataFrame when no rows match
- clear_flags deletes by ID and returns count deleted
- clear_flags with empty list returns 0, no query
- clear_flags with non-existent IDs returns 0
"""
from datetime import datetime, timezone

import duckdb
import pytest

from proto_pipe.io.db import init_all_pipeline_tables
from proto_pipe.pipelines.flagging import get_raw_flags, clear_flags


@pytest.fixture()
def conn(tmp_path):
    c = duckdb.connect(str(tmp_path / "test.db"))
    init_all_pipeline_tables(c)
    yield c
    c.close()


def _seed_source_block(conn, n=3):
    """Insert n rows into source_block. Returns list of IDs."""
    now = datetime.now(timezone.utc)
    ids = []
    for i in range(n):
        fid = f"flag-{i}"
        ids.append(fid)
        conn.execute(
            """INSERT INTO source_block
               (id, table_name, check_name, pk_value, reason, bad_columns, flagged_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [fid, "sales", "dup_check", f"PK-{i}", f"reason {i}", "col_a", now],
        )
    return ids


def _seed_validation_block(conn, n=2, report="r"):
    now = datetime.now(timezone.utc)
    ids = []
    for i in range(n):
        fid = f"vflag-{report}-{i}"
        ids.append(fid)
        conn.execute(
            """INSERT INTO validation_block
               (id, table_name, report_name, check_name, pk_value, reason, bad_columns, flagged_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [fid, "sales_report", report, "range_check", f"ORD-{i}", f"bad {i}", "price", now],
        )
    return ids


# ---------------------------------------------------------------------------
# get_raw_flags
# ---------------------------------------------------------------------------

class TestGetRawFlags:
    def test_returns_flag_aliases_for_source_block(self, conn):
        _seed_source_block(conn)
        df = get_raw_flags(conn, "source_block")
        assert "_flag_id" in df.columns
        assert "_flag_check" in df.columns
        assert "_flag_columns" in df.columns
        assert "_flag_reason" in df.columns

    def test_returns_flag_aliases_for_validation_block(self, conn):
        _seed_validation_block(conn)
        df = get_raw_flags(conn, "validation_block")
        assert "_flag_id" in df.columns
        assert "_flag_check" in df.columns
        assert "_flag_reason" in df.columns

    def test_filters_by_single_column(self, conn):
        _seed_source_block(conn)
        conn.execute(
            """INSERT INTO source_block
               (id, table_name, check_name, pk_value, reason, flagged_at)
               VALUES ('other', 'inventory', 'dup_check', 'X', 'r', CURRENT_TIMESTAMP)"""
        )
        df = get_raw_flags(conn, "source_block", filters={"table_name": "sales"})
        assert len(df) == 3
        assert all(df["table_name"] == "sales")

    def test_filters_by_multiple_columns(self, conn):
        _seed_validation_block(conn, n=2, report="r1")
        _seed_validation_block(conn, n=1, report="r2")
        # IDs collide — use unique ones
        conn.execute(
            """INSERT INTO validation_block
               (id, table_name, report_name, check_name, pk_value, reason, flagged_at)
               VALUES ('unique-1', 'sales_report', 'r2', 'null_check', 'X', 'bad', CURRENT_TIMESTAMP)"""
        )
        df = get_raw_flags(
            conn, "validation_block",
            filters={"report_name": "r2", "table_name": "sales_report"},
        )
        # r2 has the 1 seeded + 1 manually inserted
        assert len(df) >= 1
        assert all(df["report_name"] == "r2")

    def test_respects_limit(self, conn):
        _seed_source_block(conn, n=5)
        df = get_raw_flags(conn, "source_block", limit=2)
        assert len(df) == 2

    def test_empty_when_no_rows_match(self, conn):
        df = get_raw_flags(conn, "source_block", filters={"table_name": "nonexistent"})
        assert df.empty

    def test_empty_table_returns_empty_df(self, conn):
        df = get_raw_flags(conn, "source_block")
        assert df.empty


# ---------------------------------------------------------------------------
# clear_flags
# ---------------------------------------------------------------------------

class TestClearFlags:
    def test_deletes_by_id_and_returns_count(self, conn):
        ids = _seed_source_block(conn, n=3)
        cleared = clear_flags(conn, "source_block", ids[:2])
        assert cleared == 2
        remaining = conn.execute("SELECT count(*) FROM source_block").fetchone()[0]
        assert remaining == 1

    def test_empty_list_returns_zero(self, conn):
        _seed_source_block(conn)
        cleared = clear_flags(conn, "source_block", [])
        assert cleared == 0
        assert conn.execute("SELECT count(*) FROM source_block").fetchone()[0] == 3

    def test_nonexistent_ids_returns_zero(self, conn):
        _seed_source_block(conn)
        cleared = clear_flags(conn, "source_block", ["no-such-id"])
        assert cleared == 0
        assert conn.execute("SELECT count(*) FROM source_block").fetchone()[0] == 3

    def test_works_on_validation_block(self, conn):
        ids = _seed_validation_block(conn, n=2)
        cleared = clear_flags(conn, "validation_block", ids)
        assert cleared == 2
        assert conn.execute("SELECT count(*) FROM validation_block").fetchone()[0] == 0

    def test_partial_match_clears_only_matching(self, conn):
        ids = _seed_source_block(conn, n=3)
        cleared = clear_flags(conn, "source_block", [ids[0], "no-such-id"])
        assert cleared == 1
        assert conn.execute("SELECT count(*) FROM source_block").fetchone()[0] == 2
