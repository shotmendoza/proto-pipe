"""
tests/test_source_pass_source_block_invariant.py

Behavioural guarantee:
  A pk_value present in source_pass for a given table must never
  simultaneously appear in source_block for that same table.

  Enforced in ingest.py at two points:
  - Scenario A (first ingest / replace): after bulk_upsert_source_pass
  - Scenario B (_handle_duplicates): after bulk_upsert_source_pass

  Covered here:
  - TestScenarioA_ClearsSourceBlock
      accepted_row_removed_from_source_block_on_first_ingest
          A row previously in source_block is cleared when Scenario A
          accepts it (table rebuild after column type fix).

  - TestScenarioB_ClearsSourceBlock
      accepted_row_removed_from_source_block_on_reingest_upsert
          A row flagged as duplicate_conflict on one ingest is cleared
          from source_block when re-ingested and accepted via upsert mode.

      accepted_row_removed_from_source_block_on_reingest_flag
          A new row (different pk) is accepted while a previously flagged
          row for the same table is NOT accepted — the flagged row must
          remain in source_block, the accepted row must not appear there.

      no_source_block_entry_survives_for_accepted_pks
          After any successful ingest, no pk_value that appears in
          source_pass also appears in source_block for that table.
"""

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from proto_pipe.io.db import (
    init_all_pipeline_tables,
    write_registry_types,
    flag_id_for,
)
from proto_pipe.io.ingest import ingest_single_file
from proto_pipe.pipelines.flagging import FlagRecord, write_source_flags


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bootstrapped_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(db_path)
    init_all_pipeline_tables(conn)
    return conn


def _source(table: str = "sales", pk: str = "order_id", mode: str = "flag") -> dict:
    return {
        "name": "sales",
        "target_table": table,
        "patterns": ["sales_*.csv"],
        "primary_key": pk,
        "on_duplicate": mode,
    }


def _seed_source_block(conn, table: str, pk_value: str) -> None:
    """Manually plant a source_block entry — simulates a previous ingest conflict."""
    write_source_flags(conn, [FlagRecord(
        id=flag_id_for(pk_value),
        table_name=table,
        check_name="type_conflict",
        pk_value=pk_value,
        reason="simulated previous conflict",
    )])


def _in_source_block(conn, table: str, pk_value: str) -> bool:
    count = conn.execute(
        "SELECT count(*) FROM source_block WHERE table_name = ? AND pk_value = ?",
        [table, pk_value],
    ).fetchone()[0]
    return count > 0


def _in_source_pass(conn, table: str, pk_value: str) -> bool:
    count = conn.execute(
        "SELECT count(*) FROM source_pass WHERE table_name = ? AND pk_value = ?",
        [table, pk_value],
    ).fetchone()[0]
    return count > 0


# ---------------------------------------------------------------------------
# TestScenarioA_ClearsSourceBlock
# ---------------------------------------------------------------------------

class TestScenarioA_ClearsSourceBlock:
    """Scenario A: first ingest or replace mode clears stale source_block entries
    for any pk_value it accepts into source_pass."""

    def test_accepted_row_removed_from_source_block_on_first_ingest(
        self, tmp_path
    ):
        """A row previously in source_block is cleared when Scenario A accepts it.

        Guarantee:
          'A pk_value present in source_pass for a given table must never
           simultaneously appear in source_block for that same table.'
        """
        db = str(tmp_path / "pipeline.db")
        conn = _bootstrapped_conn(db)
        write_registry_types(conn, "sales", {
            "order_id": "VARCHAR",
            "amount":   "DOUBLE",
        })

        # Plant a stale source_block entry for ORD-001
        _seed_source_block(conn, "sales", "ORD-001")
        assert _in_source_block(conn, "sales", "ORD-001"), "setup: flag must exist"

        # First ingest — Scenario A fires, ORD-001 is accepted
        csv = tmp_path / "sales_2026.csv"
        csv.write_text("order_id,amount\nORD-001,99.99\nORD-002,14.50\n")
        result = ingest_single_file(conn, csv, _source())
        conn.close()

        assert result["status"] == "ok", result.get("message")

        conn = duckdb.connect(db)
        assert _in_source_pass(conn, "sales", "ORD-001"), \
            "ORD-001 must be in source_pass after successful ingest"
        assert not _in_source_block(conn, "sales", "ORD-001"), \
            "ORD-001 must NOT remain in source_block — invariant violated"
        conn.close()

    def test_replace_mode_clears_source_block(self, tmp_path):
        """Replace mode (mode='replace') also clears source_block for accepted rows."""
        db = str(tmp_path / "pipeline.db")
        conn = _bootstrapped_conn(db)
        write_registry_types(conn, "sales", {
            "order_id": "VARCHAR",
            "amount":   "DOUBLE",
        })

        # First ingest to establish the table
        csv = tmp_path / "sales_2026.csv"
        csv.write_text("order_id,amount\nORD-001,99.99\n")
        ingest_single_file(conn, csv, _source())

        # Plant a stale flag then replace
        _seed_source_block(conn, "sales", "ORD-001")
        result = ingest_single_file(conn, csv, _source(), mode="replace")
        conn.close()

        assert result["status"] == "ok", result.get("message")

        conn = duckdb.connect(db)
        assert not _in_source_block(conn, "sales", "ORD-001"), \
            "Replace mode must clear source_block for accepted rows"
        conn.close()


# ---------------------------------------------------------------------------
# TestScenarioB_ClearsSourceBlock
# ---------------------------------------------------------------------------

class TestScenarioB_ClearsSourceBlock:
    """Scenario B: subsequent ingest clears source_block entries for rows
    accepted in this run, regardless of on_duplicate mode."""

    def test_accepted_row_removed_from_source_block_on_reingest_upsert(
        self, tmp_path
    ):
        """A previously flagged row is cleared from source_block when accepted
        on re-ingest with on_duplicate='upsert'.

        Guarantee:
          'A pk_value present in source_pass for a given table must never
           simultaneously appear in source_block for that same table.'
        """
        db = str(tmp_path / "pipeline.db")
        conn = _bootstrapped_conn(db)
        write_registry_types(conn, "sales", {
            "order_id": "VARCHAR",
            "amount":   "DOUBLE",
        })

        # First ingest — establishes table and source_pass
        csv1 = tmp_path / "sales_2026_01.csv"
        csv1.write_text("order_id,amount\nORD-001,99.99\n")
        ingest_single_file(conn, csv1, _source(mode="upsert"))

        # Plant stale flag (simulates a previous conflict that was manually seeded)
        _seed_source_block(conn, "sales", "ORD-001")
        assert _in_source_block(conn, "sales", "ORD-001"), "setup: flag must exist"

        # Second ingest — upsert accepts ORD-001 again
        csv2 = tmp_path / "sales_2026_02.csv"
        csv2.write_text("order_id,amount\nORD-001,150.00\n")
        result = ingest_single_file(conn, csv2, _source(mode="upsert"))
        conn.close()

        assert result["status"] == "ok", result.get("message")

        conn = duckdb.connect(db)
        assert _in_source_pass(conn, "sales", "ORD-001"), \
            "ORD-001 must be in source_pass after upsert"
        assert not _in_source_block(conn, "sales", "ORD-001"), \
            "ORD-001 must NOT remain in source_block after being accepted"
        conn.close()

    def test_unaccepted_flagged_row_stays_in_source_block(self, tmp_path):
        """A row that is blocked (not accepted) in this run must stay in
        source_block — only accepted rows are cleared."""
        db = str(tmp_path / "pipeline.db")
        conn = _bootstrapped_conn(db)
        write_registry_types(conn, "sales", {
            "order_id": "VARCHAR",
            "amount":   "DOUBLE",
        })

        # First ingest — establish ORD-001 and ORD-002
        csv1 = tmp_path / "sales_2026_01.csv"
        csv1.write_text("order_id,amount\nORD-001,99.99\nORD-002,14.50\n")
        ingest_single_file(conn, csv1, _source(mode="flag"))

        # Second ingest — ORD-001 value changed (will be flagged), ORD-003 is new
        csv2 = tmp_path / "sales_2026_02.csv"
        csv2.write_text("order_id,amount\nORD-001,999.99\nORD-003,50.00\n")
        ingest_single_file(conn, csv2, _source(mode="flag"))
        conn.close()

        conn = duckdb.connect(db)
        # ORD-001 was blocked — must still be in source_block
        assert _in_source_block(conn, "sales", "ORD-001"), \
            "Blocked row ORD-001 must remain in source_block"
        # ORD-003 was accepted — must not be in source_block
        assert not _in_source_block(conn, "sales", "ORD-003"), \
            "Accepted row ORD-003 must not appear in source_block"
        conn.close()

    def test_no_source_block_entry_survives_for_accepted_pks(self, tmp_path):
        """After any successful ingest, the sets {source_pass pks} and
        {source_block pks} are disjoint for a given table.

        Guarantee (invariant form):
          'source_pass ∩ source_block = ∅ for any (table_name, pk_value) pair.'
        """
        db = str(tmp_path / "pipeline.db")
        conn = _bootstrapped_conn(db)
        write_registry_types(conn, "sales", {
            "order_id": "VARCHAR",
            "amount":   "DOUBLE",
        })

        # Seed stale flags for all three rows before first ingest
        for pk in ["ORD-001", "ORD-002", "ORD-003"]:
            _seed_source_block(conn, "sales", pk)

        csv = tmp_path / "sales_2026.csv"
        csv.write_text(
            "order_id,amount\nORD-001,99.99\nORD-002,14.50\nORD-003,50.00\n"
        )
        result = ingest_single_file(conn, csv, _source())
        conn.close()

        assert result["status"] == "ok", result.get("message")

        conn = duckdb.connect(db)
        pass_pks = set(
            conn.execute(
                "SELECT pk_value FROM source_pass WHERE table_name = 'sales'"
            ).df()["pk_value"].tolist()
        )
        block_pks = set(
            conn.execute(
                "SELECT pk_value FROM source_block WHERE table_name = 'sales'"
            ).df()["pk_value"].tolist()
        )
        conn.close()

        overlap = pass_pks & block_pks
        assert not overlap, (
            f"Invariant violated — pks in both source_pass and source_block: {overlap}"
        )
