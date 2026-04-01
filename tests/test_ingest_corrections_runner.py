"""
Tests covering all behaviour with the final flag identity design:

  id = md5(str(pk_value)) — deterministic, computable in DuckDB SQL,
                             no extra columns in source tables or flagged_rows.

  ingest.py:
    - TestFlagIdForHelper — md5 derivation, determinism
    - TestDirectoryValidation
    - TestNullPrimaryKeyWarning
    - TestChunking
    - TestMultipleExistingRows
    - TestOnDuplicateModes
    - TestCheckNullOverwritesIdempotent
    - TestWriteFlagIdempotent — ON CONFLICT DO NOTHING

  corrections.py:
    - TestExportFlaggedJoin — join on md5(pk_col), drift-free
    - TestImportCorrectionsXlsx
    - TestImportCorrectionsBatched
    - TestImportCorrectionsNotFound

  runner.py:
    - TestWatermarkOnlyAdvancesOnFullPass

  Integration:
    - TestFullRoundTripWithExport
"""

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

from proto_pipe.io.db import init_ingest_log, flag_id_for
from proto_pipe.io.ingest import (
    ingest_directory, _handle_duplicates, check_null_overwrites,
    CHUNK_SIZE,
    _write_flag,
)
from proto_pipe.reports.corrections import export_flagged, import_corrections, dated_export_path


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_db(tmp_path: Path) -> str:
    return str(tmp_path / "pipeline.db")


def _make_incoming(tmp_path: Path) -> Path:
    d = tmp_path / "incoming"
    d.mkdir()
    return d


def _init_flagged_rows(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the flagged_rows table — minimal schema, id is md5(pk_value)."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS flagged_rows (
            id         VARCHAR PRIMARY KEY,
            table_name VARCHAR NOT NULL,
            check_name VARCHAR,
            reason     VARCHAR,
            flagged_at TIMESTAMPTZ NOT NULL
        )
    """)


def _flag(conn, table, pk_value, reason="test reason"):
    """Insert a flag using the same md5 id derivation as _write_flag."""
    fid = flag_id_for(pk_value)
    conn.execute("""
        INSERT INTO flagged_rows (id, table_name, check_name, reason, flagged_at)
        VALUES (?, ?, 'test_check', ?, ?)
        ON CONFLICT (id) DO NOTHING
    """, [fid, table, reason, datetime.now(timezone.utc)])
    return fid


def _sources(on_duplicate="flag", primary_key="order_id"):
    return [{
        "name": "sales",
        "patterns": ["sales_*.csv"],
        "target_table": "sales",
        "timestamp_col": "updated_at",
        "primary_key": primary_key,
        "on_duplicate": on_duplicate,
    }]


def _write_csv(path: Path, rows: list[dict]) -> Path:
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _expected_flag_id(pk_value) -> str:
    return hashlib.md5(str(pk_value).encode()).hexdigest()


# ---------------------------------------------------------------------------
# TestFlagIdForHelper
# ---------------------------------------------------------------------------

class TestFlagIdForHelper:
    def test_same_pk_same_id(self):
        assert flag_id_for("ORD-1") == flag_id_for("ORD-1")

    def test_different_pk_different_id(self):
        assert flag_id_for("ORD-1") != flag_id_for("ORD-2")

    def test_matches_expected_md5(self):
        assert flag_id_for("ORD-1") == _expected_flag_id("ORD-1")

    def test_integer_pk(self):
        assert flag_id_for(42) == hashlib.md5(b"42").hexdigest()

    def test_result_is_hex_string(self):
        result = flag_id_for("ORD-1")
        assert len(result) == 32
        assert all(c in "0123456789abcdef" for c in result)


# ---------------------------------------------------------------------------
# TestWriteFlagIdempotent
# ---------------------------------------------------------------------------

class TestWriteFlagIdempotent:
    def test_same_pk_twice_produces_one_row(self):
        conn = duckdb.connect(":memory:")
        _init_flagged_rows(conn)
        _write_flag(conn, "sales", ["price: 100 -> -50"], pk_value="ORD-1")
        _write_flag(conn, "sales", ["price: 100 -> -50"], pk_value="ORD-1")
        count = conn.execute("SELECT count(*) FROM flagged_rows").fetchone()[0]
        assert count == 1

    def test_different_pk_produces_two_rows(self):
        conn = duckdb.connect(":memory:")
        _init_flagged_rows(conn)
        _write_flag(conn, "sales", ["price: 100 -> -50"], pk_value="ORD-1")
        _write_flag(conn, "sales", ["price: 200 -> -75"], pk_value="ORD-2")
        count = conn.execute("SELECT count(*) FROM flagged_rows").fetchone()[0]
        assert count == 2

    def test_no_pk_uses_uuid4_not_deduplicated(self):
        """Flags without pk_value use uuid4 — two calls produce two rows."""
        conn = duckdb.connect(":memory:")
        _init_flagged_rows(conn)
        _write_flag(conn, "sales", ["some: change"])
        _write_flag(conn, "sales", ["some: change"])
        count = conn.execute("SELECT count(*) FROM flagged_rows").fetchone()[0]
        assert count == 2

    def test_flag_id_matches_flag_id_for(self):
        conn = duckdb.connect(":memory:")
        _init_flagged_rows(conn)
        _write_flag(conn, "sales", ["price: 100 -> -50"], pk_value="ORD-1")
        stored_id = conn.execute(
            "SELECT id FROM flagged_rows"
        ).fetchone()[0]
        assert stored_id == flag_id_for("ORD-1")


# ---------------------------------------------------------------------------
# TestDirectoryValidation
# ---------------------------------------------------------------------------

class TestDirectoryValidation:
    def test_missing_directory_raises(self, tmp_path):
        db = _make_db(tmp_path)
        conn = duckdb.connect(db)
        init_ingest_log(conn)
        conn.close()
        with pytest.raises(ValueError, match="Incoming directory not found"):
            ingest_directory(str(tmp_path / "nonexistent"), [], db)

    def test_file_not_dir_raises(self, tmp_path):
        db = _make_db(tmp_path)
        not_dir = tmp_path / "file.txt"
        not_dir.write_text("hello")
        conn = duckdb.connect(db)
        init_ingest_log(conn)
        conn.close()
        with pytest.raises(ValueError, match="is not a directory"):
            ingest_directory(str(not_dir), [], db)


# ---------------------------------------------------------------------------
# TestNullPrimaryKeyWarning
# ---------------------------------------------------------------------------

class TestNullPrimaryKeyWarning:
    def test_null_key_in_file_rejects_entire_file(self, tmp_path):
        """A file with any NULL primary key values is rejected before touching the DB."""
        db = _make_db(tmp_path)
        inc = _make_incoming(tmp_path)
        _write_csv(
            inc / "sales_jan.csv",
            [
                {"order_id": None, "price": 10.0, "updated_at": "2026-01-01"},
                {"order_id": "ORD-1", "price": 20.0, "updated_at": "2026-01-02"},
            ],
        )
        summary = ingest_directory(str(inc), _sources(), db)
        assert summary["sales_jan.csv"]["status"] == "failed"
        assert "NULL" in summary["sales_jan.csv"]["message"]

    def test_null_key_logged_in_ingest_log(self, tmp_path):
        db = _make_db(tmp_path)
        inc = _make_incoming(tmp_path)
        _write_csv(
            inc / "sales_jan.csv",
            [
                {"order_id": None, "price": 10.0, "updated_at": "2026-01-01"},
            ],
        )
        ingest_directory(str(inc), _sources(), db)
        conn = duckdb.connect(db)
        row = conn.execute(
            "SELECT status, message FROM ingest_log WHERE filename = 'sales_jan.csv'"
        ).fetchone()
        conn.close()
        assert row[0] == "failed"
        assert "NULL" in row[1]

    def test_null_key_does_not_touch_existing_table(self, tmp_path):
        """Existing table is untouched when a file is rejected for NULL keys."""
        db = _make_db(tmp_path)
        inc = _make_incoming(tmp_path)
        # First ingest — clean file
        _write_csv(
            inc / "sales_jan.csv",
            [
                {"order_id": "ORD-1", "price": 100.0, "updated_at": "2026-01-01"},
            ],
        )
        ingest_directory(str(inc), _sources(), db)
        # Second ingest — file with NULL key
        _write_csv(
            inc / "sales_feb.csv",
            [
                {"order_id": None, "price": 10.0, "updated_at": "2026-02-01"},
            ],
        )
        ingest_directory(str(inc), _sources(), db)
        conn = duckdb.connect(db)
        count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        conn.close()
        assert count == 1  # original row untouched


# ---------------------------------------------------------------------------
# TestChunking
# ---------------------------------------------------------------------------

class TestChunking:
    def test_large_file_all_rows_inserted(self, tmp_path):
        db = _make_db(tmp_path)
        inc = _make_incoming(tmp_path)
        n = CHUNK_SIZE * 2 + 50
        _write_csv(inc / "sales_jan.csv", [
            {"order_id": f"ORD-{i}", "price": float(i), "updated_at": "2026-01-01"}
            for i in range(n)
        ])
        ingest_directory(str(inc), _sources(on_duplicate="append"), db)
        conn = duckdb.connect(db)
        count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        conn.close()
        assert count == n

    def test_chunk_boundary_identical_rows_deduped(self, tmp_path):
        db = _make_db(tmp_path)
        inc = _make_incoming(tmp_path)
        rows = [
            {"order_id": f"ORD-{i}", "price": float(i), "updated_at": "2026-01-01"}
            for i in range(CHUNK_SIZE + 5)
        ]
        _write_csv(inc / "sales_jan.csv", rows)
        ingest_directory(str(inc), _sources(), db)
        conn = duckdb.connect(db)
        _init_flagged_rows(conn)
        conn.close()
        _write_csv(inc / "sales_feb.csv", rows)
        ingest_directory(str(inc), _sources(), db)
        conn  = duckdb.connect(db)
        count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        conn.close()
        assert count == CHUNK_SIZE + 5


# ---------------------------------------------------------------------------
# TestMultipleExistingRows
# ---------------------------------------------------------------------------

class TestMultipleExistingRows:
    def test_no_crash_on_duplicate_keys_in_table(self, tmp_path):
        db = _make_db(tmp_path)
        inc = _make_incoming(tmp_path)
        _write_csv(inc / "sales_jan.csv", [
            {"order_id": "ORD-1", "price": 100.0, "updated_at": "2026-01-01"},
        ])
        ingest_directory(str(inc), _sources(on_duplicate="append"), db)
        _write_csv(inc / "sales_feb.csv", [
            {"order_id": "ORD-1", "price": 200.0, "updated_at": "2026-02-01"},
        ])
        ingest_directory(str(inc), _sources(on_duplicate="append"), db)

        conn = duckdb.connect(db)
        _init_flagged_rows(conn)
        df = pd.DataFrame([
            {"order_id": "ORD-1", "price": 999.0, "updated_at": "2026-03-01"}
        ])
        result_df, flagged, _ = _handle_duplicates(conn, "sales", df, "order_id", "flag")
        conn.close()
        assert flagged == 1
        assert len(result_df) == 0


# ---------------------------------------------------------------------------
# TestOnDuplicateModes
# ---------------------------------------------------------------------------

class TestOnDuplicateModes:
    def _setup(self, tmp_path, on_duplicate):
        db  = _make_db(tmp_path)
        inc = _make_incoming(tmp_path)
        _write_csv(inc / "sales_jan.csv", [
            {"order_id": "ORD-1", "price": 100.0, "updated_at": "2026-01-01"},
            {"order_id": "ORD-2", "price": 200.0, "updated_at": "2026-01-01"},
        ])
        ingest_directory(str(inc), _sources(on_duplicate=on_duplicate), db)
        return db, inc

    def test_flag_blocks_changed_row(self, tmp_path):
        db, inc = self._setup(tmp_path, "flag")
        conn = duckdb.connect(db)
        _init_flagged_rows(conn)
        conn.close()
        _write_csv(inc / "sales_feb.csv", [
            {"order_id": "ORD-1", "price": 999.0, "updated_at": "2026-02-01"},
        ])
        ingest_directory(str(inc), _sources(), db)
        conn  = duckdb.connect(db)
        price = conn.execute("SELECT price FROM sales WHERE order_id='ORD-1'").fetchone()[0]
        flags = conn.execute("SELECT count(*) FROM flagged_rows").fetchone()[0]
        conn.close()
        assert price == 100.0
        assert flags == 1

    def test_flag_passes_identical_row(self, tmp_path):
        db, inc = self._setup(tmp_path, "flag")
        conn = duckdb.connect(db)
        _init_flagged_rows(conn)
        conn.close()
        _write_csv(inc / "sales_feb.csv", [
            {"order_id": "ORD-1", "price": 100.0, "updated_at": "2026-01-01"},
        ])
        ingest_directory(str(inc), _sources(), db)
        conn  = duckdb.connect(db)
        count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        flags = conn.execute("SELECT count(*) FROM flagged_rows").fetchone()[0]
        conn.close()
        assert count == 2
        assert flags == 0

    def test_upsert_replaces_row(self, tmp_path):
        db, inc = self._setup(tmp_path, "upsert")
        _write_csv(inc / "sales_feb.csv", [
            {"order_id": "ORD-1", "price": 999.0, "updated_at": "2026-02-01"},
        ])
        ingest_directory(str(inc), _sources(on_duplicate="upsert"), db)
        conn  = duckdb.connect(db)
        price = conn.execute("SELECT price FROM sales WHERE order_id='ORD-1'").fetchone()[0]
        conn.close()
        assert price == 999.0

    def test_skip_keeps_original(self, tmp_path):
        db, inc = self._setup(tmp_path, "skip")
        _write_csv(inc / "sales_feb.csv", [
            {"order_id": "ORD-1", "price": 999.0, "updated_at": "2026-02-01"},
        ])
        ingest_directory(str(inc), _sources(on_duplicate="skip"), db)
        conn  = duckdb.connect(db)
        price = conn.execute("SELECT price FROM sales WHERE order_id='ORD-1'").fetchone()[0]
        conn.close()
        assert price == 100.0

    def test_append_allows_duplicate(self, tmp_path):
        db, inc = self._setup(tmp_path, "append")
        _write_csv(inc / "sales_feb.csv", [
            {"order_id": "ORD-1", "price": 999.0, "updated_at": "2026-02-01"},
        ])
        ingest_directory(str(inc), _sources(on_duplicate="append"), db)
        conn  = duckdb.connect(db)
        count = conn.execute(
            "SELECT count(*) FROM sales WHERE order_id='ORD-1'"
        ).fetchone()[0]
        conn.close()
        assert count == 2

    def test_flag_inserts_genuinely_new_key(self, tmp_path):
        db, inc = self._setup(tmp_path, "flag")
        conn = duckdb.connect(db)
        _init_flagged_rows(conn)
        conn.close()
        _write_csv(inc / "sales_feb.csv", [
            {"order_id": "ORD-99", "price": 50.0, "updated_at": "2026-02-01"},
        ])
        ingest_directory(str(inc), _sources(), db)
        conn  = duckdb.connect(db)
        count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        conn.close()
        assert count == 3


# ---------------------------------------------------------------------------
# TestCheckNullOverwritesIdempotent
# ---------------------------------------------------------------------------

class TestCheckNullOverwritesIdempotent:
    def test_running_twice_produces_one_flag(self, tmp_path):
        db   = _make_db(tmp_path)
        conn = duckdb.connect(db)
        _init_flagged_rows(conn)
        df = pd.DataFrame([
            {"order_id": "ORD-1", "price": 100.0},
            {"order_id": "ORD-1", "price": 200.0},
        ])
        conn.execute("CREATE TABLE sales AS SELECT * FROM df")
        first  = check_null_overwrites(conn, "sales", "order_id")
        second = check_null_overwrites(conn, "sales", "order_id")
        total  = conn.execute("SELECT count(*) FROM flagged_rows").fetchone()[0]
        conn.close()
        assert first  == 1
        assert second == 1
        assert total  == 1

    def test_finds_conflicts_across_chunk_boundary(self, tmp_path):
        db   = _make_db(tmp_path)
        conn = duckdb.connect(db)
        _init_flagged_rows(conn)
        rows = []
        for i in range(CHUNK_SIZE + 3):
            rows.append({"order_id": f"ORD-{i}", "price": float(i)})
            rows.append({"order_id": f"ORD-{i}", "price": float(i) + 1})
        df = pd.DataFrame(rows)
        conn.execute("CREATE TABLE sales AS SELECT * FROM df")
        flagged = check_null_overwrites(conn, "sales", "order_id")
        conn.close()
        assert flagged == CHUNK_SIZE + 3

    def test_returns_zero_when_no_duplicates(self, tmp_path):
        """Table with all unique primary keys returns 0."""
        db = _make_db(tmp_path)
        conn = duckdb.connect(db)
        _init_flagged_rows(conn)
        df = pd.DataFrame(
            [
                {"order_id": "ORD-1", "price": 100.0},
                {"order_id": "ORD-2", "price": 200.0},
                {"order_id": "ORD-3", "price": 300.0},
            ]
        )
        conn.execute("CREATE TABLE sales AS SELECT * FROM df")
        result = check_null_overwrites(conn, "sales", "order_id")
        conn.close()
        assert result == 0

    def test_identical_duplicate_rows_not_flagged(self, tmp_path):
        """Two rows with the same primary key but identical content produce no flag."""
        db = _make_db(tmp_path)
        conn = duckdb.connect(db)
        _init_flagged_rows(conn)
        df = pd.DataFrame(
            [
                {"order_id": "ORD-1", "price": 100.0},
                {"order_id": "ORD-1", "price": 100.0},  # exact duplicate
            ]
        )
        conn.execute("CREATE TABLE sales AS SELECT * FROM df")
        result = check_null_overwrites(conn, "sales", "order_id")
        conn.close()
        assert result == 0


# ---------------------------------------------------------------------------
# TestExportFlaggedJoin
# ---------------------------------------------------------------------------

class TestExportFlaggedJoin:
    def _make_conn(self):
        conn = duckdb.connect(":memory:")
        conn.execute("""
                     CREATE TABLE sales (order_id VARCHAR, price DOUBLE, region VARCHAR)
                     """)
        conn.execute("""
                     INSERT INTO sales VALUES
                                           ('ORD-1', 100.0, 'EMEA'),
                                           ('ORD-2', 200.0, 'APAC'),
                                           ('ORD-3', 300.0, 'LATAM')
                     """)
        _init_flagged_rows(conn)
        return conn

    def test_export_produces_csv(self, tmp_path):
        conn = self._make_conn()
        _flag(conn, "sales", "ORD-2", "price out of range")
        output = str(tmp_path / "flagged.csv")
        count = export_flagged(conn, "sales", output, primary_key="order_id")
        assert count == 1
        assert Path(output).exists()
        df = pd.read_csv(output)
        assert "_flag_id" in df.columns
        assert "_flag_reason" in df.columns
        assert df["order_id"].iloc[0] == "ORD-2"

    def test_export_correct_row_after_deletion(self, tmp_path):
        """Deleting another row does not affect which row is exported."""
        conn = self._make_conn()
        _flag(conn, "sales", "ORD-2", "negative price")
        conn.execute("DELETE FROM sales WHERE order_id = 'ORD-1'")
        output = str(tmp_path / "flagged.csv")
        export_flagged(conn, "sales", output, primary_key="order_id")
        df = pd.read_csv(output)
        assert df["order_id"].iloc[0] == "ORD-2"

    def test_export_flag_id_matches_flag_id_for(self, tmp_path):
        conn = self._make_conn()
        _flag(conn, "sales", "ORD-1", "test")
        output = str(tmp_path / "flagged.csv")
        export_flagged(conn, "sales", output, primary_key="order_id")
        df = pd.read_csv(output)
        assert df["_flag_id"].iloc[0] == flag_id_for("ORD-1")

    def test_raises_when_no_flags(self, tmp_path):
        conn = self._make_conn()
        with pytest.raises(ValueError, match="No flagged rows found"):
            export_flagged(
                conn, "sales", str(tmp_path / "out.csv"), primary_key="order_id"
            )

    def test_raises_when_primary_key_is_none(self, tmp_path):
        """export_flagged requires primary_key — None raises immediately."""
        conn = self._make_conn()
        _flag(conn, "sales", "ORD-1", "test")
        with pytest.raises(ValueError, match="primary_key is required"):
            export_flagged(conn, "sales", str(tmp_path / "out.csv"), primary_key=None)

    def test_dated_filename(self, tmp_path):
        path = dated_export_path(str(tmp_path), "sales")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert today in path
        assert path.endswith(".csv")


# ---------------------------------------------------------------------------
# TestImportCorrectionsXlsx
# ---------------------------------------------------------------------------

class TestImportCorrectionsXlsx:
    def test_accepts_xlsx(self, tmp_path):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE sales (order_id VARCHAR, price DOUBLE)")
        conn.execute("INSERT INTO sales VALUES ('ORD-1', 100.0)")
        _init_flagged_rows(conn)
        xlsx = tmp_path / "corrections.xlsx"
        pd.DataFrame([{"order_id": "ORD-1", "price": 150.0}]).to_excel(xlsx, index=False)
        result = import_corrections(conn, "sales", str(xlsx), "order_id")
        assert result["updated"] == 1
        price = conn.execute("SELECT price FROM sales").fetchone()[0]
        assert price == 150.0

    def test_rejects_unsupported_extension(self, tmp_path):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE sales (order_id VARCHAR, price DOUBLE)")
        bad  = tmp_path / "corrections.json"
        bad.write_text('[{"order_id": "ORD-1"}]')
        with pytest.raises(ValueError, match="Unsupported file type"):
            import_corrections(conn, "sales", str(bad), "order_id")


# ---------------------------------------------------------------------------
# TestImportCorrectionsBatched
# ---------------------------------------------------------------------------

class TestImportCorrectionsBatched:
    def test_large_corrections_all_applied(self, tmp_path):
        n = 500
        conn = duckdb.connect(":memory:")
        df = pd.DataFrame([{"order_id": f"ORD-{i}", "price": float(i)} for i in range(n)])
        conn.execute("CREATE TABLE sales AS SELECT * FROM df")
        _init_flagged_rows(conn)
        corr = tmp_path / "corrections.csv"
        pd.DataFrame([
            {"order_id": f"ORD-{i}", "price": float(i) + 1000} for i in range(n)
        ]).to_csv(corr, index=False)
        result = import_corrections(conn, "sales", str(corr), "order_id")
        assert result["updated"] == n
        sample = conn.execute(
            "SELECT price FROM sales WHERE order_id = 'ORD-0'"
        ).fetchone()[0]
        assert sample == 1000.0


# ---------------------------------------------------------------------------
# TestImportCorrectionsNotFound
# ---------------------------------------------------------------------------

class TestImportCorrectionsNotFound:
    def test_not_found_counted(self, tmp_path):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE sales (order_id VARCHAR, price DOUBLE)")
        conn.execute("INSERT INTO sales VALUES ('ORD-1', 100.0)")
        _init_flagged_rows(conn)
        corr = tmp_path / "corrections.csv"
        pd.DataFrame([{"order_id": "GHOST", "price": 1.0}]).to_csv(corr, index=False)
        result = import_corrections(conn, "sales", str(corr), "order_id")
        assert result["not_found"] == 1
        assert result["updated"]   == 0

    def test_found_key_updated_when_others_missing(self, tmp_path):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE sales (order_id VARCHAR, price DOUBLE)")
        conn.execute("INSERT INTO sales VALUES ('ORD-1', 100.0)")
        _init_flagged_rows(conn)
        corr = tmp_path / "corrections.csv"
        pd.DataFrame([
            {"order_id": "ORD-1", "price": 150.0},
            {"order_id": "GHOST", "price": 999.0},
        ]).to_csv(corr, index=False)
        import_corrections(conn, "sales", str(corr), "order_id")
        price = conn.execute("SELECT price FROM sales").fetchone()[0]
        assert price == 150.0


# ---------------------------------------------------------------------------
# TestWatermarkOnlyAdvancesOnFullPass
# ---------------------------------------------------------------------------

class TestWatermarkOnlyAdvancesOnFullPass:

    def test_watermark_held_when_check_fails(self, tmp_path):
        from proto_pipe.reports.runner import run_report
        from proto_pipe.checks.registry import CheckRegistry
        from proto_pipe.pipelines.watermark import WatermarkStore

        watermark_store = WatermarkStore(str(tmp_path / "watermarks.db"))
        check_registry = CheckRegistry()

        def always_fails(ctx: dict) -> pd.Series:
            raise RuntimeError("check failed at runtime")

        check_registry.register("failing_check", always_fails)

        report_config = {
            "name": "test_report",
            "source": {
                "path": ":memory:",
                "table": "sales",
                "timestamp_col": "updated_at",
            },
            "options": {"parallel": False},
            "resolved_checks": ["failing_check"],
        }
        with patch("proto_pipe.reports.runner.load_from_duckdb") as mock_load:
            mock_load.return_value = pd.DataFrame(
                [{"order_id": "ORD-1", "updated_at": "2026-01-01"}]
            )
            result = run_report(report_config, check_registry, watermark_store)

        assert watermark_store.get("test_report") is None
        assert result["results"]["failing_check"]["status"] == "error"

    def test_watermark_advances_when_all_pass(self):
        from proto_pipe.reports.runner import run_report
        from proto_pipe.checks.result import CheckResult
        import pandas as pd
        watermark_store = MagicMock()
        watermark_store.get.return_value = None
        check_registry = MagicMock()
        check_registry.run.return_value = CheckResult(
            passed=True,
            mask=pd.Series([], dtype=bool),
            reason="",
        )
        report_config = {
            "name": "test_report",
            "source": {"path": ":memory:", "table": "sales", "timestamp_col": "updated_at"},
            "options": {"parallel": False},
            "resolved_checks": ["passing_check"],
        }
        with patch("proto_pipe.reports.runner.load_from_duckdb") as mock_load:
            mock_load.return_value = pd.DataFrame([
                {"order_id": "ORD-1",
                 "updated_at": pd.Timestamp("2026-01-01", tz="UTC")}
            ])
            run_report(report_config, check_registry, watermark_store)
        watermark_store.set.assert_called_once()


# ---------------------------------------------------------------------------
# TestFullRoundTripWithExport
# ---------------------------------------------------------------------------

class TestFullRoundTripWithExport:
    def test_ingest_flag_export_fix_import(self, tmp_path):
        db  = _make_db(tmp_path)
        inc = _make_incoming(tmp_path)

        _write_csv(inc / "sales_jan.csv", [
            {"order_id": "ORD-1", "price": 100.0, "updated_at": "2026-01-01"},
            {"order_id": "ORD-2", "price": 200.0, "updated_at": "2026-01-01"},
        ])
        ingest_directory(str(inc), _sources(), db)

        conn = duckdb.connect(db)
        _init_flagged_rows(conn)
        conn.close()

        # Changed price triggers flag
        _write_csv(inc / "sales_feb.csv", [
            {"order_id": "ORD-1", "price": -50.0, "updated_at": "2026-02-01"},
        ])
        ingest_directory(str(inc), _sources(), db)

        conn       = duckdb.connect(db)
        flag_count = conn.execute("SELECT count(*) FROM flagged_rows").fetchone()[0]
        assert flag_count == 1

        # Export
        export_path = str(tmp_path / "flagged_sales.csv")
        count = export_flagged(conn, "sales", export_path, primary_key="order_id")
        assert count == 1
        assert Path(export_path).exists()

        exported = pd.read_csv(export_path)
        assert "_flag_id"     in exported.columns
        assert "_flag_reason" in exported.columns
        assert "order_id"     in exported.columns
        assert exported["order_id"].iloc[0] == "ORD-1"

        # Fix and re-import
        exported.loc[exported["order_id"] == "ORD-1", "price"] = 75.0
        exported.to_csv(export_path, index=False)

        result = import_corrections(conn, "sales", export_path, "order_id")
        conn.close()

        assert result["updated"] >= 1
        assert result["flagged_cleared"] == 1

        conn = duckdb.connect(db)
        price = conn.execute(
            "SELECT price FROM sales WHERE order_id = 'ORD-1'"
        ).fetchone()[0]
        remaining = conn.execute("SELECT count(*) FROM flagged_rows").fetchone()[0]
        conn.close()

        assert price == 75.0
        assert remaining == 0


# ---------------------------------------------------------------------------
# TestSQLFlagMode — classify query path in _handle_duplicates
# ---------------------------------------------------------------------------
class TestSQLFlagMode:
    """The flag mode classify query runs in pure SQL — no iterrows on the
    incoming DataFrame. These tests verify correctness of the SQL path."""

    def _setup(self, tmp_path):
        db = _make_db(tmp_path)
        inc = _make_incoming(tmp_path)
        _write_csv(
            inc / "sales_jan.csv",
            [
                {"order_id": "ORD-1", "price": 100.0, "updated_at": "2026-01-01"},
                {"order_id": "ORD-2", "price": 200.0, "updated_at": "2026-01-01"},
            ],
        )
        ingest_directory(str(inc), _sources(), db)
        conn = duckdb.connect(db)
        _init_flagged_rows(conn)
        conn.close()
        return db, inc

    def test_new_key_inserted(self, tmp_path):
        db, inc = self._setup(tmp_path)
        _write_csv(
            inc / "sales_feb.csv",
            [
                {"order_id": "ORD-NEW", "price": 50.0, "updated_at": "2026-02-01"},
            ],
        )
        ingest_directory(str(inc), _sources(), db)
        conn = duckdb.connect(db)
        count = conn.execute(
            "SELECT count(*) FROM sales WHERE order_id = 'ORD-NEW'"
        ).fetchone()[0]
        conn.close()
        assert count == 1

    def test_identical_row_not_inserted_not_flagged(self, tmp_path):
        """Same key, same content — silently dropped, no flag, no duplicate row."""
        db, inc = self._setup(tmp_path)
        _write_csv(
            inc / "sales_feb.csv",
            [
                {"order_id": "ORD-1", "price": 100.0, "updated_at": "2026-01-01"},
            ],
        )
        ingest_directory(str(inc), _sources(), db)
        conn = duckdb.connect(db)
        count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        flags = conn.execute("SELECT count(*) FROM flagged_rows").fetchone()[0]
        conn.close()
        assert count == 2  # no new row inserted
        assert flags == 0  # no flag raised

    def test_conflict_reason_names_changed_column(self, tmp_path):
        """The reason string built in SQL names the column that changed."""
        db, inc = self._setup(tmp_path)
        _write_csv(
            inc / "sales_feb.csv",
            [
                {"order_id": "ORD-1", "price": -50.0, "updated_at": "2026-02-01"},
            ],
        )
        ingest_directory(str(inc), _sources(), db)
        conn = duckdb.connect(db)
        reason = conn.execute("SELECT reason FROM flagged_rows").fetchone()[0]
        conn.close()
        assert "price" in reason
        assert "100.0" in reason
        assert "-50.0" in reason

    def test_conflict_id_is_md5_of_pk(self, tmp_path):
        """The flag id equals md5(pk_value) so export_flagged can join on it."""
        db, inc = self._setup(tmp_path)
        _write_csv(
            inc / "sales_feb.csv",
            [
                {"order_id": "ORD-1", "price": -50.0, "updated_at": "2026-02-01"},
            ],
        )
        ingest_directory(str(inc), _sources(), db)
        conn = duckdb.connect(db)
        stored_id = conn.execute("SELECT id FROM flagged_rows").fetchone()[0]
        conn.close()
        assert stored_id == _expected_flag_id("ORD-1")

    def test_mixed_chunk_new_identical_conflict(self, tmp_path):
        """A single file containing all three categories is handled correctly."""
        db, inc = self._setup(tmp_path)
        _write_csv(
            inc / "sales_feb.csv",
            [
                {
                    "order_id": "ORD-1",
                    "price": -50.0,
                    "updated_at": "2026-02-01",
                },  # conflict
                {
                    "order_id": "ORD-2",
                    "price": 200.0,
                    "updated_at": "2026-01-01",
                },  # identical
                {
                    "order_id": "ORD-NEW",
                    "price": 300.0,
                    "updated_at": "2026-02-01",
                },  # new
            ],
        )
        ingest_directory(str(inc), _sources(), db)
        conn = duckdb.connect(db)
        count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        flags = conn.execute("SELECT count(*) FROM flagged_rows").fetchone()[0]
        ord1 = conn.execute(
            "SELECT price FROM sales WHERE order_id = 'ORD-1'"
        ).fetchone()[0]
        new_count = conn.execute(
            "SELECT count(*) FROM sales WHERE order_id = 'ORD-NEW'"
        ).fetchone()[0]
        conn.close()
        assert count == 3  # ORD-1, ORD-2, ORD-NEW
        assert flags == 1  # only ORD-1 conflicted
        assert ord1 == 100.0  # original preserved
        assert new_count == 1  # new row inserted

    def test_conflict_row_exportable_after_flag(self, tmp_path):
        """A flagged row can be immediately exported via export_flagged."""
        db, inc = self._setup(tmp_path)
        _write_csv(
            inc / "sales_feb.csv",
            [
                {"order_id": "ORD-1", "price": -50.0, "updated_at": "2026-02-01"},
            ],
        )
        ingest_directory(str(inc), _sources(), db)
        conn = duckdb.connect(db)
        output = str(tmp_path / "flagged.csv")
        count = export_flagged(conn, "sales", output, primary_key="order_id")
        conn.close()
        assert count == 1
        df = pd.read_csv(output)
        assert df["order_id"].iloc[0] == "ORD-1"
        assert df["price"].iloc[0] == 100.0  # original price in table
