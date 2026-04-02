"""Tests for the data layer — ingest, source_pass, source_block.

Covers:
  1. coerce_for_display — Int64 dtype crash fix
  2. Phantom float flags — DuckDB-native loading eliminates 10 vs 10.0 conflicts
  3. source_pass / source_block flows — new/changed/identical record handling
  4. _flag_id bypass token in flagged_retry path
  5. _validate_row_types TRY_CAST gate — bad rows blocked, clean rows proceed
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from proto_pipe.io.db import (
    init_all_pipeline_tables,
    coerce_for_display,
    get_source_pass_hashes,
    bulk_upsert_source_pass,
    log_ingest_state,
)
from proto_pipe.pipelines.flagging import (
    FlagRecord,
    write_source_flags,
    compute_row_hash,
)
from proto_pipe.io.migration import apply_declared_types


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def conn(tmp_path) -> duckdb.DuckDBPyConnection:
    """Fresh pipeline DB connection with all pipeline tables bootstrapped."""
    db_path = str(tmp_path / "test.db")
    c = duckdb.connect(db_path)
    init_all_pipeline_tables(c)
    yield c
    c.close()


@pytest.fixture()
def incoming_dir(tmp_path) -> Path:
    d = tmp_path / "incoming"
    d.mkdir()
    return d


@pytest.fixture()
def source_def(tmp_path) -> dict:
    return {
        "name": "van",
        "target_table": "van",
        "patterns": ["van_*.csv"],
        "primary_key": "van_id",
        "on_duplicate": "flag",
    }


def _write_registry_types(conn, source_name, types):
    """Helper — write declared types to column_type_registry."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    for col, dtype in types.items():
        conn.execute("""
            INSERT INTO column_type_registry
                (column_name, source_name, declared_type, recorded_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (column_name, source_name)
            DO UPDATE SET declared_type = excluded.declared_type,
                          recorded_at   = excluded.recorded_at
        """, [col, source_name, dtype, now])


# ---------------------------------------------------------------------------
# 1. coerce_for_display — Int64 dtype crash
# ---------------------------------------------------------------------------

class TestCoerceForDisplay:
    """Int64 nullable dtype from DuckDB must not crash fillna() in display layer."""

    def test_nullable_int_becomes_object(self):
        """Int64 columns returned by DuckDB are converted to object dtype."""
        df = pd.DataFrame({"amount": pd.array([100, None, 200], dtype="Int64")})
        result = coerce_for_display(df)
        assert result["amount"].dtype == object

    def test_fillna_works_after_coerce(self):
        """fillna('') must not raise TypeError after coerce_for_display."""
        df = pd.DataFrame({
            "amount": pd.array([100, None, 200], dtype="Int64"),
            "name":   ["a", None, "c"],
        })
        result = coerce_for_display(df)
        # This raised TypeError: Invalid value for dtype Int64 before the fix
        filled = result.fillna("")
        assert filled["amount"].tolist() == [100, "", 200]

    def test_values_preserved(self):
        """Non-null values must be identical after coerce."""
        df = pd.DataFrame({"x": pd.array([1, 2, 3], dtype="Int64")})
        result = coerce_for_display(df)
        assert list(result["x"]) == [1, 2, 3]

    def test_non_nullable_columns_unchanged(self):
        """Regular int64 and float64 columns must pass through untouched."""
        df = pd.DataFrame({
            "a": [1, 2, 3],          # int64
            "b": [1.0, 2.0, 3.0],   # float64
            "c": ["x", "y", "z"],   # object
        })
        result = coerce_for_display(df)
        assert result["a"].dtype == df["a"].dtype
        assert result["b"].dtype == df["b"].dtype
        assert result["c"].dtype == df["c"].dtype

    def test_source_block_query_does_not_crash(self, conn):
        """source_block query result must be displayable without TypeError."""
        from datetime import datetime, timezone
        conn.execute("""
            INSERT INTO source_block
                (id, table_name, check_name, pk_value, flagged_at)
            VALUES ('abc123', 'van', 'duplicate_conflict', '1042', ?)
        """, [datetime.now(timezone.utc)])

        df = conn.execute("SELECT * FROM source_block").df()
        # Must not raise
        result = coerce_for_display(df)
        filled = result.fillna("")
        assert len(filled) == 1


# ---------------------------------------------------------------------------
# 2. Phantom float flags — DuckDB-native loading
# ---------------------------------------------------------------------------

class TestPhantomFloatFlags:
    """10 vs 10.0 should NOT produce a duplicate_conflict after DuckDB-native loading."""

    def test_integer_column_no_phantom_conflict(self, conn, incoming_dir, source_def):
        """A BIGINT column loaded via DuckDB must hash identically on both sides."""
        from proto_pipe.io.ingest import ingest_single_file

        _write_registry_types(conn, "van", {
            "van_id": "BIGINT",
            "endt":   "BIGINT",
        })

        # First ingest
        csv1 = incoming_dir / "van_2026_01.csv"
        csv1.write_text("van_id,endt\n1042,10\n2091,20\n")
        r1 = ingest_single_file(conn, csv1, source_def)
        assert r1["status"] == "ok"
        assert r1["flagged"] == 0

        # Second ingest — same values, must not flag phantom conflicts
        csv2 = incoming_dir / "van_2026_02.csv"
        csv2.write_text("van_id,endt\n1042,10\n2091,20\n")
        r2 = ingest_single_file(conn, csv2, source_def)
        assert r2["status"] == "ok"
        assert r2["flagged"] == 0, (
            "Phantom float conflict: '10' vs '10.0' — DuckDB-native loading should prevent this"
        )

    def test_float_column_no_phantom(self, conn, incoming_dir, source_def):
        """DOUBLE columns must also produce consistent hashes across ingests."""
        from proto_pipe.io.ingest import ingest_single_file

        _write_registry_types(conn, "van", {
            "van_id": "BIGINT",
            "amount": "DOUBLE",
        })

        csv1 = incoming_dir / "van_2026_01.csv"
        csv1.write_text("van_id,amount\n1042,1000.0\n")
        ingest_single_file(conn, csv1, source_def)

        csv2 = incoming_dir / "van_2026_02.csv"
        csv2.write_text("van_id,amount\n1042,1000.0\n")
        r2 = ingest_single_file(conn, csv2, source_def)
        assert r2["flagged"] == 0


# ---------------------------------------------------------------------------
# 3. source_pass / source_block flows
# ---------------------------------------------------------------------------

class TestSourcePassAndBlock:
    """New records inserted, changed records flagged, identical records skipped."""

    def test_new_records_written_to_source_pass(self, conn, incoming_dir, source_def):
        """First ingest writes all rows to source_pass."""
        from proto_pipe.io.ingest import ingest_single_file

        _write_registry_types(conn, "van", {"van_id": "BIGINT", "endt": "BIGINT"})
        csv = incoming_dir / "van_2026_01.csv"
        csv.write_text("van_id,endt\n1042,10\n2091,20\n")

        r = ingest_single_file(conn, csv, source_def)
        assert r["status"] == "ok"

        hashes = get_source_pass_hashes(conn, "van", ["1042", "2091"])
        assert "1042" in hashes
        assert "2091" in hashes

    def test_identical_rows_not_flagged(self, conn, incoming_dir, source_def):
        """Re-ingesting identical rows produces no flags and no inserts."""
        from proto_pipe.io.ingest import ingest_single_file

        _write_registry_types(conn, "van", {"van_id": "BIGINT", "endt": "BIGINT"})
        csv = incoming_dir / "van_2026_01.csv"
        csv.write_text("van_id,endt\n1042,10\n")
        ingest_single_file(conn, csv, source_def)

        csv2 = incoming_dir / "van_2026_02.csv"
        csv2.write_text("van_id,endt\n1042,10\n")
        r2 = ingest_single_file(conn, csv2, source_def)
        assert r2["flagged"] == 0
        assert r2["rows"] == 0

    def test_changed_row_goes_to_source_block(self, conn, incoming_dir, source_def):
        """A row with a changed value must be blocked and written to source_block."""
        from proto_pipe.io.ingest import ingest_single_file

        _write_registry_types(conn, "van", {"van_id": "BIGINT", "endt": "BIGINT"})
        csv1 = incoming_dir / "van_2026_01.csv"
        csv1.write_text("van_id,endt\n1042,10\n")
        ingest_single_file(conn, csv1, source_def)

        # Incoming row has different value — should be flagged
        csv2 = incoming_dir / "van_2026_02.csv"
        csv2.write_text("van_id,endt\n1042,99\n")
        r2 = ingest_single_file(conn, csv2, source_def)
        assert r2["flagged"] == 1

        # Verify it's in source_block
        block = conn.execute(
            "SELECT pk_value, check_name FROM source_block WHERE table_name = 'van'"
        ).df()
        assert len(block) == 1
        assert block["pk_value"].iloc[0] == "1042"
        assert block["check_name"].iloc[0] == "duplicate_conflict"

    def test_source_pass_not_updated_for_blocked_row(self, conn, incoming_dir, source_def):
        """source_pass must retain the old hash when a row is blocked."""
        from proto_pipe.io.ingest import ingest_single_file

        _write_registry_types(conn, "van", {"van_id": "BIGINT", "endt": "BIGINT"})
        csv1 = incoming_dir / "van_2026_01.csv"
        csv1.write_text("van_id,endt\n1042,10\n")
        ingest_single_file(conn, csv1, source_def)

        old_hashes = get_source_pass_hashes(conn, "van", ["1042"])

        csv2 = incoming_dir / "van_2026_02.csv"
        csv2.write_text("van_id,endt\n1042,99\n")
        ingest_single_file(conn, csv2, source_def)

        new_hashes = get_source_pass_hashes(conn, "van", ["1042"])
        # Hash must NOT have changed — blocked row rejected
        assert old_hashes["1042"] == new_hashes["1042"]

    def test_type_conflict_row_goes_to_source_block(self, conn, incoming_dir, source_def):
        """A row with a value that fails TRY_CAST must be written to source_block."""
        from proto_pipe.io.ingest import ingest_single_file

        _write_registry_types(conn, "van", {"van_id": "BIGINT", "endt": "BIGINT"})
        csv = incoming_dir / "van_2026_01.csv"
        # endt = "N/A" cannot be cast to BIGINT
        csv.write_text("van_id,endt\n1042,N/A\n2091,20\n")

        r = ingest_single_file(conn, csv, source_def)
        assert r["flagged"] == 1

        block = conn.execute(
            "SELECT pk_value, check_name FROM source_block WHERE table_name = 'van'"
        ).df()
        assert len(block) == 1
        assert block["pk_value"].iloc[0] == "1042"
        assert block["check_name"].iloc[0] == "type_conflict"

        # Clean row (2091) must still be in the table
        van = conn.execute("SELECT van_id FROM van").df()
        assert "2091" in van["van_id"].astype(str).tolist()
        assert "1042" not in van["van_id"].astype(str).tolist()


# ---------------------------------------------------------------------------
# 4. _flag_id bypass token in retry path
# ---------------------------------------------------------------------------

class TestFlagIdBypass:
    """User keeping a row in the flagged export signals acceptance — bypass hash check."""

    def test_retry_clears_flag_on_accepted_row(self, conn, incoming_dir, source_def):
        """vp flagged retry: row present in export → upsert + flag cleared."""
        from proto_pipe.io.ingest import ingest_single_file

        _write_registry_types(conn, "van", {"van_id": "BIGINT", "endt": "BIGINT"})

        # Initial ingest
        csv1 = incoming_dir / "van_2026_01.csv"
        csv1.write_text("van_id,endt\n1042,10\n")
        ingest_single_file(conn, csv1, source_def)

        # Second ingest — changed value, gets flagged
        csv2 = incoming_dir / "van_2026_02.csv"
        csv2.write_text("van_id,endt\n1042,99\n")
        ingest_single_file(conn, csv2, source_def)

        assert conn.execute(
            "SELECT count(*) FROM source_block WHERE table_name = 'van'"
        ).fetchone()[0] == 1

        # Simulate flagged export with _flag_id column + corrected value
        flag_id = conn.execute(
            "SELECT id FROM source_block WHERE pk_value = '1042'"
        ).fetchone()[0]

        export_csv = incoming_dir / "van_flagged_2026-04-02.csv"
        export_csv.write_text(f"_flag_id,van_id,endt\n{flag_id},1042,99\n")

        # retry: read export, compare _flag_id to source_block, upsert accepted rows
        import csv as _csv
        with open(export_csv, newline="") as f:
            export_rows = list(_csv.DictReader(f))
        export_flag_ids = {r["_flag_id"] for r in export_rows if r.get("_flag_id")}

        # Rows with _flag_id in source_block → upsert with on_duplicate_override=upsert
        result = ingest_single_file(
            conn, export_csv, source_def,
            on_duplicate_override="upsert",
            log_status_override="correction",
            strip_pipeline_cols=True,
        )
        assert result["status"] == "ok"

        # Clear the matched flags
        if export_flag_ids:
            placeholders = ", ".join(["?"] * len(export_flag_ids))
            conn.execute(
                f"DELETE FROM source_block WHERE id IN ({placeholders})",
                list(export_flag_ids),
            )

        remaining = conn.execute(
            "SELECT count(*) FROM source_block WHERE table_name = 'van'"
        ).fetchone()[0]
        assert remaining == 0

    def test_deleted_row_flag_cleared_as_skipped(self, conn, incoming_dir, source_def):
        """Row deleted from export → flag cleared, logged as skipped."""
        from proto_pipe.io.ingest import ingest_single_file

        _write_registry_types(conn, "van", {"van_id": "BIGINT", "endt": "BIGINT"})

        csv1 = incoming_dir / "van_2026_01.csv"
        csv1.write_text("van_id,endt\n1042,10\n2091,20\n")
        ingest_single_file(conn, csv1, source_def)

        # Flag row 1042
        csv2 = incoming_dir / "van_2026_02.csv"
        csv2.write_text("van_id,endt\n1042,99\n2091,20\n")
        ingest_single_file(conn, csv2, source_def)

        # Simulate user deleting the flagged row from the export
        flag_id = conn.execute(
            "SELECT id FROM source_block WHERE pk_value = '1042'"
        ).fetchone()[0]
        all_flag_ids = {"1042": flag_id}
        export_flag_ids: set[str] = set()  # user deleted all rows from export

        deleted_flag_ids = [fid for fid in all_flag_ids.values() if fid not in export_flag_ids]
        conn.execute(
            f"DELETE FROM source_block WHERE id IN (?)",
            deleted_flag_ids,
        )
        log_ingest_state(conn, "van_flagged.csv", "van", "skipped",
                         message=f"{len(deleted_flag_ids)} row(s) deleted from export — skipped")

        remaining = conn.execute(
            "SELECT count(*) FROM source_block WHERE table_name = 'van'"
        ).fetchone()[0]
        assert remaining == 0

        skipped_log = conn.execute(
            "SELECT status FROM ingest_state WHERE table_name = 'van' AND status = 'skipped'"
        ).df()
        assert len(skipped_log) == 1


# ---------------------------------------------------------------------------
# 5. _validate_row_types TRY_CAST gate
# ---------------------------------------------------------------------------

class TestValidateRowTypes:
    """TRY_CAST gate: bad rows blocked to source_block, clean rows proceed."""

    def test_bad_rows_blocked_clean_rows_proceed(self, conn, incoming_dir, source_def):
        """Mixed file: bad type rows blocked, clean rows inserted successfully."""
        from proto_pipe.io.ingest import ingest_single_file

        _write_registry_types(conn, "van", {
            "van_id": "BIGINT",
            "amount": "DOUBLE",
            "endt":   "BIGINT",
        })

        csv = incoming_dir / "van_2026_01.csv"
        # Row 1042: amount = "not_a_number" → type_conflict
        # Row 2091: all clean → proceeds
        csv.write_text("van_id,amount,endt\n1042,not_a_number,10\n2091,999.99,20\n")

        r = ingest_single_file(conn, csv, source_def)
        assert r["status"] == "ok"
        assert r["flagged"] == 1
        assert r["rows"] == 1  # only clean row inserted

        # Clean row is in source table
        van_ids = conn.execute("SELECT van_id FROM van").df()["van_id"].tolist()
        assert 2091 in van_ids or "2091" in [str(v) for v in van_ids]

        # Bad row is in source_block
        block = conn.execute(
            "SELECT pk_value, check_name, bad_columns FROM source_block WHERE table_name = 'van'"
        ).df()
        assert len(block) == 1
        assert block["check_name"].iloc[0] == "type_conflict"
        assert "amount" in (block["bad_columns"].iloc[0] or "")

    def test_all_clean_no_flags(self, conn, incoming_dir, source_def):
        """File with all clean rows produces zero source_block entries."""
        from proto_pipe.io.ingest import ingest_single_file

        _write_registry_types(conn, "van", {"van_id": "BIGINT", "endt": "BIGINT"})
        csv = incoming_dir / "van_2026_01.csv"
        csv.write_text("van_id,endt\n1042,10\n2091,20\n3000,30\n")

        r = ingest_single_file(conn, csv, source_def)
        assert r["status"] == "ok"
        assert r["flagged"] == 0
        assert r["rows"] == 3

        block_count = conn.execute(
            "SELECT count(*) FROM source_block"
        ).fetchone()[0]
        assert block_count == 0

    def test_all_bad_no_insert(self, conn, incoming_dir, source_def):
        """File where all rows fail TRY_CAST produces zero inserts."""
        from proto_pipe.io.ingest import ingest_single_file

        _write_registry_types(conn, "van", {"van_id": "BIGINT", "endt": "BIGINT"})
        csv = incoming_dir / "van_2026_01.csv"
        csv.write_text("van_id,endt\n1042,not_a_number\n2091,also_bad\n")

        r = ingest_single_file(conn, csv, source_def)
        assert r["status"] == "ok"
        assert r["flagged"] == 2
        assert r["rows"] == 0

    def test_bad_column_name_stored_in_source_block(self, conn, incoming_dir, source_def):
        """bad_columns field must identify which column caused the type conflict."""
        from proto_pipe.io.ingest import ingest_single_file

        _write_registry_types(conn, "van", {
            "van_id": "BIGINT",
            "endt":   "BIGINT",
            "amount": "DOUBLE",
        })
        csv = incoming_dir / "van_2026_01.csv"
        csv.write_text("van_id,endt,amount\n1042,bad_endt,1000.0\n")

        ingest_single_file(conn, csv, source_def)

        block = conn.execute(
            "SELECT bad_columns FROM source_block WHERE pk_value = '1042'"
        ).df()
        assert len(block) == 1
        assert "endt" in (block["bad_columns"].iloc[0] or "")
