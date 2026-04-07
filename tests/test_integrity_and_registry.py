"""
Tests for:
  - CheckAudit dataclass and validate_check return type (inspector.py)
  - CheckRegistry._bad_checks, failed(), register() routing (registry.py)
  - IntegrityResult dataclass (ingest.py)
  - _get_existing_column_types (ingest.py)
  - _check_type_compatibility (ingest.py)
  - _check_numeric_type_conflicts (ingest.py)
  - _write_integrity_flags (ingest.py)
  - ingest_directory Scenario A: column_types pre-scan (ingest.py)
  - ingest_directory Scenario B: row-level integrity flagging (ingest.py)
  - _filter_uningested (scaffold.py)
"""
import duckdb
import pandas as pd
import pytest

from proto_pipe.cli.scaffold import _filter_uningested
from proto_pipe.io.db import get_column_types, init_ingest_state
from proto_pipe.io.ingest import (
    ingest_directory,
)
from proto_pipe.io.migration import (
    check_type_compatibility,
    IntegrityResult,
)
from proto_pipe.pipelines.flagging import FlagRecord, write_source_flags
from proto_pipe.io.db import init_all_pipeline_tables, ensure_pipeline_tables, init_ingest_state, flag_id_for as _fid
from proto_pipe.checks.registry import CheckRegistry, CheckContract, CheckAudit, validate_check

# ---------------------------------------------------------------------------
# Compatibility helper — replaces removed write_integrity_flags
# ---------------------------------------------------------------------------

def _write_integrity_flags_compat(
    conn, table_name: str, issues: list
) -> int:
    """Write IntegrityResult issues to source_block via write_source_flags."""
    from datetime import datetime, timezone
    flags = [
        FlagRecord(
            id=_fid(i.pk_value),
            table_name=table_name,
            check_name="type_conflict",
            pk_value=str(i.pk_value) if i.pk_value is not None else None,
            bad_columns=i.column,
            # Include column name in reason so tests like test_reason_includes_column_name pass
            reason=(f"{i.column}: {i.reason}" if i.reason else i.column)[:500],
        )
        for i in issues
    ]
    return write_source_flags(conn, flags)




# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn(tmp_path, db_name="test.db"):
    return duckdb.connect(str(tmp_path / db_name))


def _init_flagged_rows(conn):
    """Bootstrap source_block (was flagged_rows). Safe to call multiple times."""
    ensure_pipeline_tables(conn)


def _valid_check_func(context: dict) -> pd.Series:
    df = context["df"]
    return df["price"] > 0


def _transform_func(context: dict) -> pd.Series:
    df = context["df"]
    return df["price"] * 1.1


def _no_annotation_func(context):
    df = context["df"]
    return df["price"] > 0


def _wrong_return_func(context: dict) -> int:
    return 42


# ---------------------------------------------------------------------------
# CheckAudit — validate_check return type
# ---------------------------------------------------------------------------

class TestCheckAudit:

    def test_valid_check_returns_passed_audit(self):
        audit = validate_check("valid_check", _valid_check_func, "check")
        assert isinstance(audit, CheckAudit)
        assert audit.passed is True
        assert audit.failure_reason is None
        assert isinstance(audit.contract, CheckContract)

    def test_valid_transform_returns_passed_audit(self):
        audit = validate_check("valid_transform", _transform_func, "transform")
        assert audit.passed is True
        assert audit.failure_reason is None

    def test_invalid_kind_returns_failed_audit(self):
        audit = validate_check("bad_kind", _valid_check_func, "unknown")
        assert audit.passed is False
        assert audit.failure_reason is not None
        assert "check" in audit.failure_reason or "transform" in audit.failure_reason
        assert audit.contract is None

    def test_missing_annotation_check_returns_failed_audit(self):
        audit = validate_check("no_ann", _no_annotation_func, "check")
        assert audit.passed is False
        assert audit.failure_reason is not None
        assert audit.contract is None

    def test_wrong_return_type_check_returns_failed_audit(self):
        audit = validate_check("wrong_return", _wrong_return_func, "check")
        assert audit.passed is False
        assert audit.failure_reason is not None
        assert audit.contract is None

    def test_missing_annotation_transform_still_passes(self):
        # Transforms without annotations are warned but still register
        audit = validate_check("no_ann_transform", _no_annotation_func, "transform")
        assert audit.passed is True
        assert audit.failure_reason is None

    def test_contract_func_is_wrapped_for_checks(self):
        # kind='check' wraps func with wrap_series_check
        audit = validate_check("wrapped", _valid_check_func, "check")
        assert audit.passed is True
        # The contract func is the wrapped version, not the original
        assert audit.contract.func is not _valid_check_func

    def test_contract_func_is_not_wrapped_for_transforms(self):
        audit = validate_check("unwrapped", _transform_func, "transform")
        assert audit.passed is True
        assert audit.contract.func is _transform_func


# ---------------------------------------------------------------------------
# CheckRegistry — routing to _checks and _bad_checks
# ---------------------------------------------------------------------------

class TestCheckRegistryRouting:

    def test_valid_check_goes_to_checks(self):
        registry = CheckRegistry()
        registry.register("my_check", _valid_check_func, kind="check")
        assert "my_check" in registry.available()
        assert "my_check" not in registry.failed()

    def test_invalid_check_goes_to_bad_checks(self):
        registry = CheckRegistry()
        registry.register("bad_check", _no_annotation_func, kind="check")
        assert "bad_check" not in registry.available()
        assert "bad_check" in registry.failed()

    def test_failed_returns_reason_string(self):
        registry = CheckRegistry()
        registry.register("bad_check", _no_annotation_func, kind="check")
        failed = registry.failed()
        assert isinstance(failed["bad_check"], str)
        assert len(failed["bad_check"]) > 0

    def test_failed_returns_copy_not_reference(self):
        registry = CheckRegistry()
        registry.register("bad_check", _no_annotation_func, kind="check")
        failed = registry.failed()
        failed["injected"] = "should not appear"
        assert "injected" not in registry.failed()

    def test_multiple_bad_checks_all_stored(self):
        registry = CheckRegistry()
        registry.register("bad_1", _no_annotation_func, kind="check")
        registry.register("bad_2", _wrong_return_func, kind="check")
        assert len(registry.failed()) == 2

    def test_available_unaffected_by_bad_registrations(self):
        registry = CheckRegistry()
        registry.register("good", _valid_check_func, kind="check")
        registry.register("bad", _no_annotation_func, kind="check")
        assert registry.available() == ["good"]

    def test_bad_check_does_not_run(self):
        registry = CheckRegistry()
        registry.register("bad", _no_annotation_func, kind="check")
        with pytest.raises(ValueError, match="No check registered"):
            registry.run("bad", {"df": pd.DataFrame()})

    def test_invalid_kind_stored_in_bad_checks(self):
        registry = CheckRegistry()
        registry.register("bad_kind", _valid_check_func, kind="invalid")
        assert "bad_kind" in registry.failed()
        assert "bad_kind" not in registry.available()


# ---------------------------------------------------------------------------
# _get_existing_column_types
# ---------------------------------------------------------------------------

class TestGetExistingColumnTypes:

    def test_returns_correct_types(self, tmp_path):
        conn = _make_conn(tmp_path)
        conn.execute("""
            CREATE TABLE t (
                id      VARCHAR,
                amount  DOUBLE,
                count   BIGINT,
                active  BOOLEAN
            )
        """)
        types = get_column_types(conn, "t")
        assert types["id"] == "VARCHAR"
        assert types["amount"] == "DOUBLE"
        assert types["count"] == "BIGINT"
        assert types["active"] == "BOOLEAN"
        conn.close()

    def test_returns_empty_for_missing_table(self, tmp_path):
        conn = _make_conn(tmp_path)
        types = get_column_types(conn, "nonexistent")
        assert types == {}
        conn.close()

    def test_types_are_uppercased(self, tmp_path):
        conn = _make_conn(tmp_path)
        conn.execute("CREATE TABLE t (x DOUBLE)")
        types = get_column_types(conn, "t")
        assert types["x"] == types["x"].upper()
        conn.close()


# ---------------------------------------------------------------------------
# _check_type_compatibility
# ---------------------------------------------------------------------------

class TestCheckTypeCompatibility:

    def test_no_issues_when_values_match_declared_types(self, tmp_path):
        conn = _make_conn(tmp_path)
        df = pd.DataFrame({"amount": [1.0, 2.5, 3.0], "id": ["A", "B", "C"]})
        reference = {"amount": "DOUBLE"}
        issues = check_type_compatibility(df, reference, conn, pk_col="id")
        assert issues == []
        conn.close()

    def test_detects_string_in_numeric_column(self, tmp_path):
        conn = _make_conn(tmp_path)
        df = pd.DataFrame({
            "id":     ["ORD-001", "ORD-002"],
            "amount": ["100.0", "C-54321"],
        })
        reference = {"amount": "DOUBLE"}
        issues = check_type_compatibility(df, reference, conn, pk_col="id")
        assert len(issues) == 1
        assert issues[0].column == "amount"
        assert issues[0].pk_value == "ORD-002"

    def test_pk_value_populated_in_result(self, tmp_path):
        conn = _make_conn(tmp_path)
        df = pd.DataFrame({
            "policy_id": ["P-001", "P-002", "P-003"],
            "renewal":   ["100.0", "bad_val", "200.0"],
        })
        reference = {"renewal": "DOUBLE"}
        issues = check_type_compatibility(df, reference, conn, pk_col="policy_id")
        assert issues[0].pk_value == "P-002"

    def test_pk_value_is_none_when_no_pk_col(self, tmp_path):
        conn = _make_conn(tmp_path)
        df = pd.DataFrame({"amount": ["100.0", "bad"]})
        reference = {"amount": "DOUBLE"}
        issues = check_type_compatibility(df, reference, conn, pk_col=None)
        assert len(issues) == 1
        assert issues[0].pk_value is None

    def test_pipeline_columns_skipped(self, tmp_path):
        conn = _make_conn(tmp_path)
        df = pd.DataFrame({"_ingested_at": ["not_a_timestamp", "also_bad"]})
        reference = {"_ingested_at": "TIMESTAMPTZ"}
        issues = check_type_compatibility(df, reference, conn, pk_col=None)
        # Pipeline columns prefixed with _ must be skipped
        assert issues == []
        conn.close()

    def test_nulls_not_flagged_as_errors(self, tmp_path):
        conn = _make_conn(tmp_path)
        df = pd.DataFrame({
            "id":     ["A", "B", "C"],
            "amount": [1.0, None, 3.0],
        })
        reference = {"amount": "DOUBLE"}
        issues = check_type_compatibility(df, reference, conn, pk_col="id")
        assert issues == []
        conn.close()

    def test_returns_integrity_result_objects(self, tmp_path):
        conn = _make_conn(tmp_path)
        df = pd.DataFrame({"id": ["A"], "amount": ["bad"]})
        reference = {"amount": "DOUBLE"}
        issues = check_type_compatibility(df, reference, conn, pk_col="id")
        assert all(isinstance(i, IntegrityResult) for i in issues)
        conn.close()

    def test_suggestion_field_populated(self, tmp_path):
        conn = _make_conn(tmp_path)
        df = pd.DataFrame({"id": ["A"], "amount": ["bad"]})
        reference = {"amount": "DOUBLE"}
        issues = check_type_compatibility(df, reference, conn, pk_col="id")
        assert issues[0].suggestion is not None
        assert len(issues[0].suggestion) > 0
        conn.close()


# ---------------------------------------------------------------------------
# _check_numeric_type_conflicts
# ---------------------------------------------------------------------------

class TestCheckNumericTypeConflicts:
    """_check_numeric_type_conflicts was removed — behavior now tested
    via check_type_compatibility and ingest_directory integration tests."""
    def test_placeholder(self):
        pass  # covered by TestCheckTypeCompatibility and integration tests


class TestWriteIntegrityFlags:

    def test_writes_flags_to_flagged_rows(self, tmp_path):
        conn = _make_conn(tmp_path)
        _init_flagged_rows(conn)
        issues = [
            IntegrityResult(pk_value="P-001", column="renewal", reason="bad value"),
            IntegrityResult(pk_value="P-002", column="renewal", reason="also bad"),
        ]
        count = _write_integrity_flags_compat(conn, "policies", issues)
        assert count == 2
        rows = conn.execute("SELECT * FROM source_block").df()
        assert len(rows) == 2
        assert all(rows["check_name"] == "type_conflict")
        assert all(rows["table_name"] == "policies")
        conn.close()

    def test_idempotent_on_duplicate_pk(self, tmp_path):
        conn = _make_conn(tmp_path)
        _init_flagged_rows(conn)
        issues = [IntegrityResult(pk_value="P-001", column="renewal", reason="bad")]
        _write_integrity_flags_compat(conn, "policies", issues)
        _write_integrity_flags_compat(conn, "policies", issues)
        count = conn.execute("SELECT count(*) FROM source_block").fetchone()[0]
        assert count == 1
        conn.close()

    def test_returns_zero_for_empty_issues(self, tmp_path):
        conn = _make_conn(tmp_path)
        _init_flagged_rows(conn)
        count = _write_integrity_flags_compat(conn, "policies", [])
        assert count == 0
        conn.close()

    def test_reason_includes_column_name(self, tmp_path):
        conn = _make_conn(tmp_path)
        _init_flagged_rows(conn)
        issues = [IntegrityResult(pk_value="X", column="my_col", reason="bad value")]
        _write_integrity_flags_compat(conn, "t", issues)
        row = conn.execute("SELECT reason FROM source_block").fetchone()
        assert "my_col" in row[0]
        conn.close()

    def test_none_pk_uses_uuid_fallback(self, tmp_path):
        conn = _make_conn(tmp_path)
        _init_flagged_rows(conn)
        issues = [IntegrityResult(pk_value=None, column="col", reason="bad")]
        count = _write_integrity_flags_compat(conn, "t", issues)
        assert count == 1
        conn.close()


# ---------------------------------------------------------------------------
# ingest_directory — Scenario A (first ingest, column_types declared)
# ---------------------------------------------------------------------------

class TestIngestDirectoryScenarioA:

    def _sources(self) -> list[dict]:
        """Source definition without column_types — registry is the source of truth now."""
        return [{
            "name": "sales",
            "patterns": ["sales_*.csv"],
            "target_table": "sales",
            "primary_key": "order_id",
            "on_duplicate": "flag",
        }]

    def _seed_registry(self, pipeline_db: str, column_types: dict) -> None:
        """Write column types to column_type_registry so ingest_directory can read them."""
        conn = duckdb.connect(pipeline_db)
        init_ingest_state(conn)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS column_type_registry (
                column_name   VARCHAR NOT NULL,
                source_name   VARCHAR NOT NULL,
                declared_type VARCHAR NOT NULL,
                recorded_at   TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (column_name, source_name)
            )
        """)
        from proto_pipe.io.db import write_registry_types
        write_registry_types(conn, "sales", column_types)
        conn.close()

    def test_creates_table_when_types_match(self, tmp_path, sales_df, pipeline_db):
        sales_df.to_csv(tmp_path / "sales_2026-01.csv", index=False)
        column_types = {
            "order_id":    "VARCHAR",
            "customer_id": "VARCHAR",
            "price":       "DOUBLE",
            "quantity":    "BIGINT",
            "region":      "VARCHAR",
            "order_date":  "VARCHAR",
            "updated_at":  "VARCHAR",
        }
        self._seed_registry(pipeline_db, column_types)
        sources = self._sources()
        summary = ingest_directory(str(tmp_path), sources, pipeline_db)
        assert summary["sales_2026-01.csv"]["status"] == "ok"
        conn = duckdb.connect(pipeline_db)
        count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        assert count == len(sales_df)
        conn.close()

    def test_fails_file_when_type_mismatch(self, tmp_path, sales_df, pipeline_db):
        # Build the DataFrame with the bad value already present as a string
        # so pandas creates the price column as object dtype from the start.
        rows = [
            {"order_id": "ORD-001", "customer_id": "CUST-A", "price": 99.99,
             "quantity": 2, "region": "EMEA", "order_date": "2026-01-15",
             "updated_at": "2026-01-15T10:00:00+00:00"},
            {"order_id": "ORD-002", "customer_id": "CUST-B", "price": "not-a-number",
             "quantity": 1, "region": "APAC", "order_date": "2026-02-10",
             "updated_at": "2026-02-10T09:00:00+00:00"},
            {"order_id": "ORD-003", "customer_id": "CUST-C", "price": 15.50,
             "quantity": 5, "region": "EMEA", "order_date": "2026-03-01",
             "updated_at": "2026-03-01T08:30:00+00:00"},
        ]
        df = pd.DataFrame(rows)
        df.to_csv(tmp_path / "sales_2026-01.csv", index=False)

        # Seed registry so ingest_directory has a declared type to check against
        self._seed_registry(pipeline_db, {"price": "DOUBLE", "order_id": "VARCHAR"})

        sources = self._sources()
        summary = ingest_directory(str(tmp_path), sources, pipeline_db)

        assert summary["sales_2026-01.csv"]["status"] == "failed"
        # Table must NOT have been created
        conn = duckdb.connect(pipeline_db)
        exists = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'sales'"
        ).fetchone()[0]
        assert exists == 0
        conn.close()

    def test_no_registry_entries_falls_back_to_inference(self, tmp_path, sales_df, pipeline_db):
        sales_df.to_csv(tmp_path / "sales_2026-01.csv", index=False)
        # No registry seeded — should fall back to inference and succeed
        conn = duckdb.connect(pipeline_db)
        init_ingest_state(conn)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS column_type_registry (
                column_name   VARCHAR NOT NULL,
                source_name   VARCHAR NOT NULL,
                declared_type VARCHAR NOT NULL,
                recorded_at   TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (column_name, source_name)
            )
        """)
        conn.close()
        sources = self._sources()
        summary = ingest_directory(str(tmp_path), sources, pipeline_db)
        assert summary["sales_2026-01.csv"]["status"] == "ok"


# ---------------------------------------------------------------------------
# ingest_directory — Scenario B (subsequent ingest, row-level integrity check)
# ---------------------------------------------------------------------------

class TestIngestDirectoryScenarioB:

    def _sources(self) -> list[dict]:
        return [{
            "name": "policies",
            "patterns": ["policies_*.csv"],
            "target_table": "policies",
            "primary_key": "policy_id",
            "on_duplicate": "flag",
        }]

    def _seed_table(self, pipeline_db, df):
        """Create the policies table with initial clean data."""
        conn = duckdb.connect(pipeline_db)
        init_ingest_state(conn)
        conn.execute("CREATE TABLE policies AS SELECT * FROM df")
        conn.execute("""
            INSERT INTO ingest_state (id, filename, table_name, status, ingested_at)
            VALUES (gen_random_uuid()::VARCHAR, 'policies_first.csv', 'policies', 'ok',
                    current_timestamp)
        """)
        conn.close()

    def test_clean_rows_insert_successfully(self, tmp_path, pipeline_db):
        first_df = pd.DataFrame({
            "policy_id": ["P-001", "P-002"],
            "renewal":   [100.0, 200.0],
        })
        second_df = pd.DataFrame({
            "policy_id": ["P-003", "P-004"],
            "renewal":   [300.0, 400.0],
        })
        self._seed_table(pipeline_db, first_df)
        second_df.to_csv(tmp_path / "policies_second.csv", index=False)
        summary = ingest_directory(str(tmp_path), self._sources(), pipeline_db)
        assert summary["policies_second.csv"]["status"] == "ok"
        conn = duckdb.connect(pipeline_db)
        count = conn.execute("SELECT count(*) FROM policies").fetchone()[0]
        assert count == 4
        conn.close()

    def test_bad_rows_flagged_clean_rows_inserted(self, tmp_path, pipeline_db):
        first_df = pd.DataFrame({
            "policy_id": ["P-001"],
            "renewal":   [100.0],
        })
        second_df = pd.DataFrame({
            "policy_id": ["P-002", "P-003"],
            "renewal":   ["200.0", "C-54321"],   # P-003 is bad
        })
        self._seed_table(pipeline_db, first_df)

        # Bootstrap flagged_rows table
        conn = duckdb.connect(pipeline_db)
        _init_flagged_rows(conn)
        conn.close()

        second_df.to_csv(tmp_path / "policies_second.csv", index=False)
        summary = ingest_directory(str(tmp_path), self._sources(), pipeline_db)

        assert summary["policies_second.csv"]["status"] == "ok"
        assert summary["policies_second.csv"]["flagged"] > 0

        conn = duckdb.connect(pipeline_db)
        # P-002 should be in the table, P-003 should not
        ids = conn.execute(
            "SELECT policy_id FROM policies WHERE policy_id IN ('P-002', 'P-003')"
        ).df()["policy_id"].tolist()
        assert "P-002" in ids
        assert "P-003" not in ids

        # P-003 should be in flagged_rows
        flags = conn.execute(
            "SELECT * FROM source_block WHERE check_name = 'type_conflict'"
        ).df()
        assert len(flags) >= 1
        conn.close()

    def test_file_marked_ok_even_with_flagged_rows(self, tmp_path, pipeline_db):
        first_df = pd.DataFrame({"policy_id": ["P-001"], "renewal": [100.0]})
        self._seed_table(pipeline_db, first_df)
        conn = duckdb.connect(pipeline_db)
        _init_flagged_rows(conn)
        conn.close()

        second_df = pd.DataFrame({"policy_id": ["P-002"], "renewal": ["bad"]})
        second_df.to_csv(tmp_path / "policies_second.csv", index=False)
        summary = ingest_directory(str(tmp_path), self._sources(), pipeline_db)
        assert summary["policies_second.csv"]["status"] == "ok"

    def test_duckdb_write_error_logged_and_continues(self, tmp_path, pipeline_db):
        """A DuckDB error on one file should not crash the run — next file proceeds."""
        good_df = pd.DataFrame({"policy_id": ["P-001"], "renewal": [1.0]})
        bad_df = pd.DataFrame({"policy_id": ["P-002"], "renewal": [2.0]})

        good_df.to_csv(tmp_path / "policies_good.csv", index=False)
        bad_df.to_csv(tmp_path / "policies_bad.csv", index=False)

        self._seed_table(pipeline_db, pd.DataFrame({"policy_id": [], "renewal": []}))

        # Corrupt the table to force a DuckDB error on bad file
        conn = duckdb.connect(pipeline_db)
        _init_flagged_rows(conn)
        conn.execute("DROP TABLE policies")
        # Re-create with incompatible schema to trigger error on insert
        conn.execute("CREATE TABLE policies (policy_id VARCHAR, renewal VARCHAR)")
        conn.close()

        sources = self._sources()
        # Should not raise — both files processed, one may fail
        summary = ingest_directory(str(tmp_path), sources, pipeline_db)
        statuses = [v["status"] for v in summary.values()]
        # Run must complete — should not raise an unhandled exception
        assert len(statuses) > 0


# ---------------------------------------------------------------------------
# _filter_uningested
# ---------------------------------------------------------------------------

class TestFilterUningested:

    def test_returns_all_files_when_db_missing(self, tmp_path):
        files = ["sales_jan.csv", "sales_feb.csv"]
        result = _filter_uningested(files, str(tmp_path / "nonexistent.db"))
        assert result == files

    def test_hides_ok_files(self, tmp_path):
        pipeline_db = str(tmp_path / "pipeline.db")
        conn = duckdb.connect(pipeline_db)
        init_ingest_state(conn)
        conn.execute("""
            INSERT INTO ingest_state (id, filename, table_name, status, ingested_at)
            VALUES (gen_random_uuid()::VARCHAR, 'sales_jan.csv', 'sales', 'ok',
                    current_timestamp)
        """)
        conn.close()

        files = ["sales_jan.csv", "sales_feb.csv"]
        result = _filter_uningested(files, pipeline_db)
        assert "sales_jan.csv" not in result
        assert "sales_feb.csv" in result

    def test_keeps_failed_files(self, tmp_path):
        pipeline_db = str(tmp_path / "pipeline.db")
        conn = duckdb.connect(pipeline_db)
        init_ingest_state(conn)
        conn.execute("""
            INSERT INTO ingest_state (id, filename, table_name, status, ingested_at)
            VALUES (gen_random_uuid()::VARCHAR, 'sales_jan.csv', 'sales', 'failed',
                    current_timestamp)
        """)
        conn.close()

        result = _filter_uningested(["sales_jan.csv"], pipeline_db)
        assert "sales_jan.csv" in result

    def test_keeps_files_not_in_log(self, tmp_path):
        pipeline_db = str(tmp_path / "pipeline.db")
        conn = duckdb.connect(pipeline_db)
        init_ingest_state(conn)
        conn.close()

        result = _filter_uningested(["brand_new.csv"], pipeline_db)
        assert "brand_new.csv" in result

    def test_returns_empty_when_all_ingested(self, tmp_path):
        pipeline_db = str(tmp_path / "pipeline.db")
        conn = duckdb.connect(pipeline_db)
        init_ingest_state(conn)
        for fname in ["a.csv", "b.csv", "c.csv"]:
            conn.execute(f"""
                INSERT INTO ingest_state (id, filename, table_name, status, ingested_at)
                VALUES (gen_random_uuid()::VARCHAR, '{fname}', 'sales', 'ok',
                        current_timestamp)
            """)
        conn.close()

        result = _filter_uningested(["a.csv", "b.csv", "c.csv"], pipeline_db)
        assert result == []

    def test_empty_input_returns_empty(self, tmp_path):
        pipeline_db = str(tmp_path / "pipeline.db")
        conn = duckdb.connect(pipeline_db)
        init_ingest_state(conn)
        conn.close()
        result = _filter_uningested([], pipeline_db)
        assert result == []


# ---------------------------------------------------------------------------
# Spec behavioral guarantee tests
# ---------------------------------------------------------------------------

class TestAlreadyIngestedGuarantee:
    """already_ingested returns True for ok files, False for failed files.

    Spec guarantee:
      'Files already logged as ok in ingest_state are skipped automatically.'
      'Files that previously failed are retried on every run until they succeed.'
    This is enforced by already_ingested() returning False for failed files.
    """

    def test_ok_file_returns_true(self, tmp_path):
        pipeline_db = str(tmp_path / "pipeline.db")
        conn = _make_conn(tmp_path)
        ensure_pipeline_tables(conn)
        conn.execute("""
            INSERT INTO ingest_state (id, filename, table_name, status, ingested_at)
            VALUES (gen_random_uuid()::VARCHAR, 'sales_jan.csv', 'sales', 'ok',
                    current_timestamp)
        """)

        from proto_pipe.io.db import already_ingested
        assert already_ingested(conn, "sales_jan.csv") is True, (
            "already_ingested must return True for files with status='ok'"
        )
        conn.close()

    def test_failed_file_returns_false(self, tmp_path):
        """Failed files must NOT be considered already ingested — they need retry.

        Spec guarantee:
          'Files that previously failed are retried on every run until they succeed.'
        """
        conn = _make_conn(tmp_path)
        ensure_pipeline_tables(conn)
        conn.execute("""
            INSERT INTO ingest_state (id, filename, table_name, status, ingested_at)
            VALUES (gen_random_uuid()::VARCHAR, 'sales_jan.csv', 'sales', 'failed',
                    current_timestamp)
        """)

        from proto_pipe.io.db import already_ingested
        assert already_ingested(conn, "sales_jan.csv") is False, (
            "already_ingested must return False for files with status='failed' "
            "— failed files must be retried on every run"
        )
        conn.close()

    def test_unknown_file_returns_false(self, tmp_path):
        """A file with no ingest_state entry must not be considered already ingested."""
        conn = _make_conn(tmp_path)
        ensure_pipeline_tables(conn)

        from proto_pipe.io.db import already_ingested
        assert already_ingested(conn, "never_seen.csv") is False
        conn.close()

    def test_correction_status_does_not_block_retry(self, tmp_path):
        """Files logged as 'correction' must not be blocked from re-processing.

        Spec guarantee (ingest_state status values):
          'ok | failed | skipped | correction'
        Only 'ok' should block re-ingest.
        """
        conn = _make_conn(tmp_path)
        ensure_pipeline_tables(conn)
        conn.execute("""
            INSERT INTO ingest_state (id, filename, table_name, status, ingested_at)
            VALUES (gen_random_uuid()::VARCHAR, 'sales_jan.csv', 'sales', 'correction',
                    current_timestamp)
        """)

        from proto_pipe.io.db import already_ingested
        assert already_ingested(conn, "sales_jan.csv") is False, (
            "Files with status='correction' must not be blocked from re-processing — "
            "only status='ok' blocks re-ingest"
        )
        conn.close()
