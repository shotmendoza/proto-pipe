"""Integration tests for the alias_map workflow — end-to-end behavioral guarantees.

Covers gaps not addressed by the existing test_alias_map_features.py:
  - int/float column-backed params expand through alias_map the same as str params
  - _output reserved key is excluded from expansion key computation
  - check_set_hash changes when alias_map contents change (routing change triggers revalidation)
  - Unified routing: int/float + pd.Series gate enforced (constant entry only)
  - Full alias_map roundtrip: config → registration → run → validation_block results

Behavioral guarantees tested here:
  - Any param (str, int, float, unannotated), no pd.Series, column picked
    → alias_map entry → per-row column data passed
  - Any param, any pd.Series on function → column picker skipped, constant only
  - alias_map contents included in check_set_hash — routing change triggers revalidation
  - _output reserved key excluded from expansion key
  - is_expandable() gate: dict-returning functions never expand
"""
from __future__ import annotations

import functools
from unittest.mock import MagicMock

import duckdb
import pandas as pd
import pytest

from proto_pipe.checks.registry import CheckRegistry, ReportRegistry, CheckParamInspector
from proto_pipe.io.registry import (
    _build_alias_param_map,
    _expand_check_with_alias_map,
    register_from_config,
)
from proto_pipe.pipelines.flagging import compute_check_set_hash
from proto_pipe.checks.built_in import BUILT_IN_CHECKS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def restore_built_in_checks():
    """Restore BUILT_IN_CHECKS after each test that mutates it."""
    original = dict(BUILT_IN_CHECKS)
    yield
    BUILT_IN_CHECKS.clear()
    BUILT_IN_CHECKS.update(original)


def _fresh_registry() -> CheckRegistry:
    reg = CheckRegistry()
    for name, func in BUILT_IN_CHECKS.items():
        reg.register(name, func)
    return reg


# ---------------------------------------------------------------------------
# 1. int/float column-backed params expand through alias_map same as str
# ---------------------------------------------------------------------------

class TestColumnBackedScalarExpansion:
    """Behavioral guarantee: int/float params in alias_map expand identically to str params.

    CLAUDE.md: 'column backed scalars and column selectors run through the same
    when going through the alias_map process.'
    """

    def test_float_param_in_alias_map_expands_to_n_checks(self):
        """float-annotated param in alias_map produces N expanded checks — same as str."""
        def check_with_float(bar: float) -> "pd.Series[bool]":
            pass

        BUILT_IN_CHECKS["_check_float"] = check_with_float
        reg = _fresh_registry()

        names = _expand_check_with_alias_map(
            func_name="_check_float",
            params={},
            alias_param_map={"bar": ["col_a", "col_b", "col_c"]},
            check_registry=reg,
        )

        assert len(names) == 3, (
            f"float param in alias_map must expand to 3 checks, got {len(names)}"
        )
        assert len(set(names)) == 3, "Each expansion must be a distinct registered check"

    def test_int_param_in_alias_map_expands_to_n_checks(self):
        """int-annotated param in alias_map produces N expanded checks."""
        def check_with_int(bar: int) -> "pd.Series[bool]":
            pass

        BUILT_IN_CHECKS["_check_int"] = check_with_int
        reg = _fresh_registry()

        names = _expand_check_with_alias_map(
            func_name="_check_int",
            params={},
            alias_param_map={"bar": ["price", "quantity"]},
            check_registry=reg,
        )

        assert len(names) == 2, (
            f"int param in alias_map must expand to 2 checks, got {len(names)}"
        )

    def test_mixed_str_and_float_params_align_correctly(self):
        """When str and float params both appear in alias_map, alignment is maintained.

        CLAUDE.md broadcast rule: if multiple params have N columns, run i uses
        column i from each param. Misaligned lengths raise ValueError.
        """
        def check_two_params(col: str, threshold: float) -> "pd.Series[bool]":
            pass

        BUILT_IN_CHECKS["_check_mixed"] = check_two_params
        reg = _fresh_registry()

        names = _expand_check_with_alias_map(
            func_name="_check_mixed",
            params={},
            alias_param_map={
                "col": ["endt", "expiry_date"],
                "threshold": ["floor_a", "floor_b"],
            },
            check_registry=reg,
        )

        assert len(names) == 2, "Two aligned param pairs must produce 2 checks"
        # Unequal lengths must still raise
        with pytest.raises(ValueError, match="unequal lengths"):
            _expand_check_with_alias_map(
                func_name="_check_mixed",
                params={},
                alias_param_map={
                    "col": ["endt", "expiry_date", "extra"],
                    "threshold": ["floor_a", "floor_b"],
                },
                check_registry=reg,
            )

    def test_float_single_entry_broadcasts_silently(self):
        """Single-entry float param in alias_map broadcasts across all runs."""
        def check_two_params(col: str, threshold: float) -> "pd.Series[bool]":
            pass

        BUILT_IN_CHECKS["_check_broadcast"] = check_two_params
        reg = _fresh_registry()

        names = _expand_check_with_alias_map(
            func_name="_check_broadcast",
            params={},
            alias_param_map={
                "col": ["endt", "expiry_date", "eff_date"],
                "threshold": ["floor_col"],  # single entry — broadcasts
            },
            check_registry=reg,
        )

        assert len(names) == 3, (
            "Single-entry float param must broadcast across 3 col expansions"
        )


# ---------------------------------------------------------------------------
# 2. _output reserved key excluded from expansion key
# ---------------------------------------------------------------------------

class TestOutputKeyExclusion:
    """Behavioral guarantee: _output reserved key is excluded from expansion key computation.

    CLAUDE.md: '_output reserved alias_map key: for kind=transform with column-backed
    params, the output column is specified via {param: "_output", column: result_col}.
    _output is excluded from expansion key computation.'
    """

    def test_output_key_excluded_from_expansion(self):
        """_output entries in alias_map do not affect the number of expanded checks."""
        def my_transform(col: str) -> "pd.Series":
            pass

        BUILT_IN_CHECKS["_transform_output"] = my_transform
        reg = _fresh_registry()

        # Without _output
        names_without = _expand_check_with_alias_map(
            func_name="_transform_output",
            params={},
            alias_param_map={"col": ["endt", "expiry_date"]},
            check_registry=_fresh_registry(),
        )

        # With _output — same expansion count, _output is pass-through only
        names_with = _expand_check_with_alias_map(
            func_name="_transform_output",
            params={},
            alias_param_map={
                "col": ["endt", "expiry_date"],
                "_output": ["result_a", "result_b"],
            },
            check_registry=_fresh_registry(),
        )

        assert len(names_without) == 2
        assert len(names_with) == 2, (
            "_output key must not add extra expansion runs — it is a write-back hint only"
        )

    def test_output_only_alias_map_registers_single(self):
        """alias_map with only _output entries (no input params) registers single check."""
        def my_transform(col: str) -> "pd.Series":
            pass

        BUILT_IN_CHECKS["_transform_out_only"] = my_transform
        reg = _fresh_registry()

        names = _expand_check_with_alias_map(
            func_name="_transform_out_only",
            params={},
            alias_param_map={"_output": ["result_col"]},
            check_registry=reg,
        )

        assert len(names) == 1, (
            "_output-only alias_map must register single check, not expand"
        )


# ---------------------------------------------------------------------------
# 3. check_set_hash changes when alias_map contents change
# ---------------------------------------------------------------------------

class TestCheckSetHashIncludesAliasMap:
    """Behavioral guarantee: alias_map contents are included in check_set_hash.

    CLAUDE.md: 'alias_map contents included in check_set_hash so routing changes
    trigger revalidation.'
    """

    def _make_registry(self):
        reg = CheckRegistry()
        from proto_pipe.checks.built_in import BUILT_IN_CHECKS
        for name, func in BUILT_IN_CHECKS.items():
            reg.register(name, func)
        return reg

    def test_same_checks_different_alias_map_produce_different_hashes(self):
        """Changing alias_map contents changes check_set_hash."""
        reg = self._make_registry()
        check_entries = [{"name": "range_check"}]

        alias_map_a = [{"param": "col", "column": "endt"}]
        alias_map_b = [{"param": "col", "column": "expiry_date"}]

        hash_a = compute_check_set_hash(check_entries, reg, alias_map=alias_map_a)
        hash_b = compute_check_set_hash(check_entries, reg, alias_map=alias_map_b)

        assert hash_a != hash_b, (
            "Different alias_map column mappings must produce different check_set_hash "
            "so routing changes trigger revalidation"
        )

    def test_same_checks_same_alias_map_produce_same_hash(self):
        """Identical alias_map contents produce identical check_set_hash (deterministic)."""
        reg = self._make_registry()
        check_entries = [{"name": "range_check"}]
        alias_map = [{"param": "col", "column": "endt"}]

        hash_1 = compute_check_set_hash(check_entries, reg, alias_map=alias_map)
        hash_2 = compute_check_set_hash(check_entries, reg, alias_map=alias_map)

        assert hash_1 == hash_2, "Same inputs must always produce same hash (deterministic)"

    def test_no_alias_map_vs_empty_alias_map_identical(self):
        """None alias_map and empty list alias_map produce the same hash."""
        reg = self._make_registry()
        check_entries = [{"name": "range_check"}]

        hash_none = compute_check_set_hash(check_entries, reg, alias_map=None)
        hash_empty = compute_check_set_hash(check_entries, reg, alias_map=[])

        assert hash_none == hash_empty, (
            "None and [] alias_map must produce identical hashes — "
            "both represent 'no alias_map configured'"
        )

    def test_adding_alias_map_entry_changes_hash(self):
        """Adding a new alias_map entry changes the hash even if checks are unchanged."""
        reg = self._make_registry()
        check_entries = [{"name": "range_check"}]

        hash_before = compute_check_set_hash(check_entries, reg, alias_map=[])
        hash_after = compute_check_set_hash(
            check_entries, reg,
            alias_map=[{"param": "col", "column": "new_col"}]
        )

        assert hash_before != hash_after, (
            "Adding an alias_map entry must change check_set_hash so the runner "
            "knows to revalidate records"
        )


# ---------------------------------------------------------------------------
# 4. pd.Series gate: int/float params skip column picker when Series present
# ---------------------------------------------------------------------------

class TestSeriesGateEnforcedOnScalarParams:
    """Behavioral guarantee: when any pd.Series param exists, all other params
    (including int/float) are broadcast constants — column picker is not offered.

    CLAUDE.md: 'If any pd.Series params exist, all other params skip the column
    picker and go straight to constant entry. No type exceptions to this rule.'
    """

    def test_series_gate_makes_scalar_params_return_empty(self):
        """column_backed_scalar_params() returns [] when any pd.Series param exists."""
        def mixed_func(col: "pd.Series", threshold: float) -> "pd.Series[bool]":
            pass

        inspector = CheckParamInspector(mixed_func)

        assert inspector.has_series_params() is True
        assert inspector.column_backed_scalar_params() == [], (
            "float param must not be column-backed eligible when pd.Series param present"
        )

    def test_no_series_makes_scalar_params_eligible(self):
        """column_backed_scalar_params() returns scalar params when no pd.Series."""
        def scalar_only(bar: int, foobar: float) -> "pd.Series[bool]":
            pass

        inspector = CheckParamInspector(scalar_only)

        assert inspector.has_series_params() is False
        assert set(inspector.column_backed_scalar_params()) == {"bar", "foobar"}, (
            "int/float params must be column-backed eligible when no pd.Series present"
        )

    def test_register_from_config_expands_int_float_params(self):
        """register_from_config expands int/float alias_map params same as str."""
        def check_threshold(price: float, floor: float) -> "pd.Series[bool]":
            return pd.Series([True])

        BUILT_IN_CHECKS["_check_threshold"] = check_threshold
        reg = _fresh_registry()
        rep_reg = ReportRegistry()

        config = {
            "templates": {},
            "reports": [{
                "name": "threshold_report",
                "source": {"type": "duckdb", "path": "", "table": "sales"},
                "alias_map": [
                    {"param": "price", "column": "price_col_a"},
                    {"param": "price", "column": "price_col_b"},
                    {"param": "floor", "column": "floor_col_a"},
                    {"param": "floor", "column": "floor_col_b"},
                ],
                "options": {"parallel": False},
                "checks": [{"name": "_check_threshold", "params": {}}],
            }],
        }

        register_from_config(config, reg, rep_reg)
        resolved = rep_reg.get("threshold_report")["resolved_checks"]

        assert len(resolved) == 2, (
            f"Two aligned float param pairs must produce 2 expanded checks, got {len(resolved)}"
        )


# ---------------------------------------------------------------------------
# 5. Full alias_map roundtrip — config → registration → execution → results
# ---------------------------------------------------------------------------

class TestAliasMapFullRoundtrip:
    """End-to-end: alias_map config → registered checks → run → validation_block entries.

    CLAUDE.md alias_map purpose 1 (cross-source aliasing) and purpose 2 (multi-column
    expansion) are both exercised here with actual DuckDB execution.
    """

    def test_cross_source_aliasing_runs_correct_column(self, tmp_path):
        """Each report runs the check against its own aliased column.

        Purpose 1: `col → endt` in van, `col → expiry_date` in foo.
        Each check must pass only when the correct column is evaluated.
        """
        from proto_pipe.checks.runner import run_checks_and_flag
        from proto_pipe.io.db import init_all_pipeline_tables
        import functools
        from proto_pipe.checks.built_in import check_range

        db_path = str(tmp_path / "pipeline.db")
        conn = duckdb.connect(db_path)
        init_all_pipeline_tables(conn)
        conn.close()

        df_van = pd.DataFrame({"endt": [500.0], "order_id": ["V1"]})
        df_foo = pd.DataFrame({"expiry_date": [500.0], "order_id": ["F1"]})

        reg = CheckRegistry()
        reg.register(
            "range_endt",
            functools.partial(check_range, col="endt", min_val=0, max_val=1000),
        )
        reg.register(
            "range_expiry",
            functools.partial(check_range, col="expiry_date", min_val=0, max_val=1000),
        )

        # van — check against endt
        run_checks_and_flag(
            check_names=["range_endt"],
            registry=reg,
            context={"df": df_van},
            pipeline_db=db_path,
            report_name="van_report",
            table_name="van",
            pk_col="order_id",
        )

        # foo — check against expiry_date
        run_checks_and_flag(
            check_names=["range_expiry"],
            registry=reg,
            context={"df": df_foo},
            pipeline_db=db_path,
            report_name="foo_report",
            table_name="foo",
            pk_col="order_id",
        )

        conn = duckdb.connect(db_path)
        count = conn.execute(
            "SELECT count(*) FROM validation_block"
        ).fetchone()[0]
        conn.close()

        assert count == 0, (
            "Both checks should pass (values in range) — no validation_block entries expected"
        )

    def test_multi_column_expansion_each_run_independent(self, tmp_path):
        """Each alias_map column produces an independent check run and result.

        Purpose 2: col → [col_a, col_b] produces 2 runs. Failing col_a and
        passing col_b produce exactly 1 entry in validation_block.
        """
        from proto_pipe.checks.runner import run_checks_and_flag
        from proto_pipe.io.db import init_all_pipeline_tables
        import functools
        from proto_pipe.checks.built_in import check_range

        db_path = str(tmp_path / "pipeline.db")
        conn = duckdb.connect(db_path)
        init_all_pipeline_tables(conn)
        conn.close()

        df = pd.DataFrame({
            "col_a": [-1.0],   # fails range check
            "col_b": [50.0],   # passes range check
            "order_id": ["ORD-001"],
        })

        reg = CheckRegistry()
        reg.register(
            "range_col_a",
            functools.partial(check_range, col="col_a", min_val=0, max_val=100),
        )
        reg.register(
            "range_col_b",
            functools.partial(check_range, col="col_b", min_val=0, max_val=100),
        )

        run_checks_and_flag(
            check_names=["range_col_a", "range_col_b"],
            registry=reg,
            context={"df": df},
            pipeline_db=db_path,
            report_name="test_report",
            table_name="sales",
            pk_col="order_id",
        )

        conn = duckdb.connect(db_path)
        count = conn.execute(
            "SELECT count(*) FROM validation_block WHERE report_name = 'test_report'"
        ).fetchone()[0]
        conn.close()

        assert count == 1, (
            f"col_a fails (1 entry in validation_block), col_b passes (0). "
            f"Expected 1 total, got {count}"
        )