"""Tests for CheckParamInspector and multi-select registry expansion.

Covers:
- CheckParamInspector: unwraps wrapped functions, partials, and wrapped partials
- CheckParamInspector: column_params, scalar_params, is_multiselect_eligible
- registry resolve_check_uuid: list param expansion into multiple registrations
- registry resolve_check_uuid: scalar broadcast across expansions
- registry resolve_check_uuid: single-length list broadcast
- registry resolve_check_uuid: incompatible lengths truncate to shortest
- registry resolve_check_uuid: no list params leaves behaviour unchanged
- registry resolve_check_uuid: each expansion gets a unique key
- settings: multi_select_params defaults to True
"""

from functools import partial

import pandas as pd
import pytest

from proto_pipe.checks.result import wrap_series_check
from proto_pipe.checks.registry import CheckRegistry, ReportRegistry, CheckParamInspector


# ---------------------------------------------------------------------------
# Sample check functions for testing
# ---------------------------------------------------------------------------

def check_price(context, col: str, min_val: float, max_val: float) -> pd.Series:
    df = context["df"]
    return (df[col] >= min_val) & (df[col] <= max_val)


def check_no_params(context) -> pd.Series:
    return pd.Series([True] * len(context["df"]))


def check_multi_col(context, col_a: str, col_b: str) -> pd.Series:
    df = context["df"]
    return df[col_a] > df[col_b]


def check_dict_return(context, col: str):
    """Returns dict — not eligible for multiselect."""
    return {"violations": 0}


# ---------------------------------------------------------------------------
# CheckParamInspector — unwrapping
# ---------------------------------------------------------------------------

class TestCheckParamInspectorUnwrapping:
    def test_plain_function(self):
        inspector = CheckParamInspector(check_price)
        assert inspector.func is check_price

    def test_unwraps_partial(self):
        fn = partial(check_price, col="price", min_val=0, max_val=500)
        inspector = CheckParamInspector(fn)
        assert inspector.func is check_price

    def test_unwraps_wrapped_function(self):
        wrapped = wrap_series_check(check_price)
        inspector = CheckParamInspector(wrapped)
        assert inspector.func is check_price

    def test_unwraps_wrapped_partial(self):
        """Worst case: wrap_series_check(partial(func, ...))."""
        fn = partial(check_price, col="price", min_val=0, max_val=500)
        wrapped = wrap_series_check(fn)
        inspector = CheckParamInspector(wrapped)
        assert inspector.func is check_price

    def test_signature_is_original(self):
        wrapped = wrap_series_check(check_price)
        inspector = CheckParamInspector(wrapped)
        params = list(inspector._sig.parameters.keys())
        assert "col" in params
        assert "min_val" in params
        assert "max_val" in params


# ---------------------------------------------------------------------------
# CheckParamInspector — param classification
# ---------------------------------------------------------------------------

class TestCheckParamInspectorClassification:
    def test_column_params_returns_str_params(self):
        inspector = CheckParamInspector(check_price)
        assert inspector.column_params() == ["col"]

    def test_scalar_params_returns_non_str_params(self):
        inspector = CheckParamInspector(check_price)
        assert set(inspector.scalar_params()) == {"min_val", "max_val"}

    def test_no_params_gives_empty_lists(self):
        inspector = CheckParamInspector(check_no_params)
        assert inspector.column_params() == []
        assert inspector.scalar_params() == []

    def test_multiple_col_params(self):
        inspector = CheckParamInspector(check_multi_col)
        assert set(inspector.column_params()) == {"col_a", "col_b"}

    def test_returns_boolean_series_true(self):
        inspector = CheckParamInspector(check_price)
        assert inspector.returns_boolean_series() is True

    def test_returns_boolean_series_false_no_annotation(self):
        inspector = CheckParamInspector(check_dict_return)
        assert inspector.returns_boolean_series() is False

    def test_is_multiselect_eligible_true(self):
        inspector = CheckParamInspector(check_price)
        assert inspector.is_multiselect_eligible() is True

    def test_is_multiselect_eligible_false_no_col_params(self):
        inspector = CheckParamInspector(check_no_params)
        assert inspector.is_multiselect_eligible() is False

    def test_is_multiselect_eligible_false_no_series_return(self):
        inspector = CheckParamInspector(check_dict_return)
        assert inspector.is_multiselect_eligible() is False

    def test_is_multiselect_eligible_through_registry(self):
        """is_multiselect_eligible works on a function retrieved from the registry."""
        registry = CheckRegistry()
        registry.register("price_check", check_price)
        func = registry.get("price_check")
        inspector = CheckParamInspector(func)
        assert inspector.is_multiselect_eligible() is True

    def test_is_multiselect_eligible_through_registry_with_params(self):
        """is_multiselect_eligible works when registered with partial params."""
        registry = CheckRegistry()
        registry.register("price_check", partial(check_price, min_val=0, max_val=500))
        func = registry.get("price_check")
        inspector = CheckParamInspector(func)
        assert inspector.is_multiselect_eligible() is True


# ---------------------------------------------------------------------------
# registry resolve_check_uuid — list param expansion
# ---------------------------------------------------------------------------

@pytest.fixture()
def registry():
    from proto_pipe.checks.built_in import BUILT_IN_CHECKS
    BUILT_IN_CHECKS["price_check"] = check_price
    BUILT_IN_CHECKS["multi_col_check"] = check_multi_col
    BUILT_IN_CHECKS["no_params_check"] = check_no_params
    r = CheckRegistry()
    r.register("price_check", check_price)
    yield r
    BUILT_IN_CHECKS.pop("price_check", None)
    BUILT_IN_CHECKS.pop("multi_col_check", None)
    BUILT_IN_CHECKS.pop("no_params_check", None)


@pytest.fixture()
def minimal_report():
    """Helper to build a minimal report dict with inline checks."""
    def _make(checks):
        return {"name": "test_report", "checks": checks}
    return _make


class TestResolveCheckUuidExpansion:
    """Column param expansion via alias_map in register_from_config."""

    def _config(self, alias_map, checks):
        return {
            "templates": {},
            "reports": [{
                "name": "test_report",
                "source": {"type": "duckdb", "path": "", "table": "t"},
                "alias_map": alias_map,
                "options": {"parallel": False},
                "checks": checks,
            }],
        }

    def _register_test_checks(self):
        from proto_pipe.checks.built_in import BUILT_IN_CHECKS
        BUILT_IN_CHECKS["price_check"] = check_price
        BUILT_IN_CHECKS["multi_col_check"] = check_multi_col

    def _cleanup_test_checks(self):
        from proto_pipe.checks.built_in import BUILT_IN_CHECKS
        BUILT_IN_CHECKS.pop("price_check", None)
        BUILT_IN_CHECKS.pop("multi_col_check", None)

    def test_no_alias_map_single_registration(self, registry, minimal_report):
        from proto_pipe.io.registry import resolve_check_uuid
        report = minimal_report([
            {"name": "price_check", "params": {"col": "price", "min_val": 0, "max_val": 500}}
        ])
        names = resolve_check_uuid(report, registry)
        assert len(names) == 1

    def test_alias_map_expands_to_multiple(self):
        from proto_pipe.io.registry import register_from_config
        self._register_test_checks()
        try:
            reg, rep_reg = CheckRegistry(), ReportRegistry()
            config = self._config(
                alias_map=[
                    {"param": "col", "column": "price"},
                    {"param": "col", "column": "cost"},
                    {"param": "col", "column": "fee"},
                ],
                checks=[{"name": "price_check", "params": {"min_val": 0, "max_val": 500}}],
            )
            register_from_config(config, reg, rep_reg)
            assert len(rep_reg.get("test_report")["resolved_checks"]) == 3
        finally:
            self._cleanup_test_checks()

    def test_scalar_params_broadcast_across_expansions(self):
        from proto_pipe.io.registry import register_from_config, _build_check_keys
        self._register_test_checks()
        try:
            reg, rep_reg = CheckRegistry(), ReportRegistry()
            config = self._config(
                alias_map=[
                    {"param": "col", "column": "price"},
                    {"param": "col", "column": "cost"},
                ],
                checks=[{"name": "price_check", "params": {"min_val": 0, "max_val": 500}}],
            )
            register_from_config(config, reg, rep_reg)
            names = rep_reg.get("test_report")["resolved_checks"]
            expected_price = _build_check_keys("price_check", {"col": "price", "min_val": 0, "max_val": 500})
            expected_cost = _build_check_keys("price_check", {"col": "cost", "min_val": 0, "max_val": 500})
            assert expected_price in names
            assert expected_cost in names
        finally:
            self._cleanup_test_checks()

    def test_single_length_list_produces_one_registration(self):
        from proto_pipe.io.registry import register_from_config
        self._register_test_checks()
        try:
            reg, rep_reg = CheckRegistry(), ReportRegistry()
            config = self._config(
                alias_map=[{"param": "col", "column": "price"}],
                checks=[{"name": "price_check", "params": {"min_val": 0, "max_val": 500}}],
            )
            register_from_config(config, reg, rep_reg)
            assert len(rep_reg.get("test_report")["resolved_checks"]) == 1
        finally:
            self._cleanup_test_checks()

    def test_single_entry_broadcasts_with_multi_entry(self):
        """col_a: [x] with col_b: [a, b] → broadcast col_a to [x, x]."""
        from proto_pipe.io.registry import register_from_config
        self._register_test_checks()
        try:
            reg, rep_reg = CheckRegistry(), ReportRegistry()
            config = self._config(
                alias_map=[
                    {"param": "col_a", "column": "x"},
                    {"param": "col_b", "column": "a"},
                    {"param": "col_b", "column": "b"},
                ],
                checks=[{"name": "multi_col_check"}],
            )
            register_from_config(config, reg, rep_reg)
            assert len(rep_reg.get("test_report")["resolved_checks"]) == 2
        finally:
            self._cleanup_test_checks()

    def test_incompatible_lengths_raises(self):
        """Multi-entry params with unequal lengths should raise ValueError."""
        from proto_pipe.io.registry import register_from_config
        self._register_test_checks()
        try:
            reg, rep_reg = CheckRegistry(), ReportRegistry()
            config = self._config(
                alias_map=[
                    {"param": "col_a", "column": "x"},
                    {"param": "col_a", "column": "y"},
                    {"param": "col_b", "column": "a"},
                    {"param": "col_b", "column": "b"},
                    {"param": "col_b", "column": "c"},
                ],
                checks=[{"name": "multi_col_check"}],
            )
            with pytest.raises(ValueError, match="unequal lengths"):
                register_from_config(config, reg, rep_reg)
        finally:
            self._cleanup_test_checks()

    def test_no_params_check_unchanged(self, minimal_report):
        from proto_pipe.io.registry import resolve_check_uuid
        from proto_pipe.checks.built_in import BUILT_IN_CHECKS
        BUILT_IN_CHECKS["no_params_check"] = check_no_params
        try:
            r = CheckRegistry()
            r.register("no_params_check", check_no_params)
            report = minimal_report([{"name": "no_params_check"}])
            names = resolve_check_uuid(report, r)
            assert len(names) == 1
        finally:
            BUILT_IN_CHECKS.pop("no_params_check", None)

    def test_template_not_expanded(self, minimal_report):
        """Template references pass through unchanged."""
        r = CheckRegistry()
        r.register("my_template", check_price)
        report = minimal_report([{"template": "my_template"}])
        from proto_pipe.io.registry import resolve_check_uuid
        names = resolve_check_uuid(report, r)
        assert names == ["my_template"]

    def test_each_expansion_registered_in_registry(self):
        from proto_pipe.io.registry import register_from_config
        self._register_test_checks()
        try:
            reg, rep_reg = CheckRegistry(), ReportRegistry()
            config = self._config(
                alias_map=[
                    {"param": "col", "column": "price"},
                    {"param": "col", "column": "cost"},
                ],
                checks=[{"name": "price_check", "params": {"min_val": 0, "max_val": 500}}],
            )
            register_from_config(config, reg, rep_reg)
            names = rep_reg.get("test_report")["resolved_checks"]
            for name in names:
                assert name in reg.available()
        finally:
            self._cleanup_test_checks()


# ---------------------------------------------------------------------------
# settings — multi_select_params default
# ---------------------------------------------------------------------------

class TestMultiSelectParamsSetting:
    def test_defaults_to_true(self):
        from proto_pipe.io.config import load_settings
        settings = load_settings()
        assert settings.get("multi_select_params") is True

# ---------------------------------------------------------------------------
# Spec behavioral guarantee tests
# ---------------------------------------------------------------------------

class TestMultiselectGatePreventExpansion:
    """is_multiselect_eligible() gates expansion — non-eligible functions are not expanded.

    Spec guarantee:
      'CheckParamInspector.is_multiselect_eligible() gates whether a function
       supports multi-column expansion.'
    """

    def test_non_eligible_function_not_expanded_via_register_from_config(self):
        """A function without str column params must not expand even with alias_map.

        check_no_params has no column params → is_multiselect_eligible()=False
        → alias_map must be ignored, single registration produced.
        """
        from proto_pipe.io.registry import register_from_config
        from proto_pipe.checks.built_in import BUILT_IN_CHECKS

        BUILT_IN_CHECKS["no_params_check"] = check_no_params
        try:
            reg, rep_reg = CheckRegistry(), ReportRegistry()
            config = {
                "templates": {},
                "reports": [{
                    "name": "test_report",
                    "source": {"type": "duckdb", "path": "", "table": "t"},
                    "alias_map": [
                        {"param": "col", "column": "Coverage A"},
                        {"param": "col", "column": "Coverage B"},
                        {"param": "col", "column": "Coverage C"},
                    ],
                    "options": {"parallel": False},
                    "checks": [{"name": "no_params_check"}],
                }],
            }
            register_from_config(config, reg, rep_reg)
            resolved = rep_reg.get("test_report")["resolved_checks"]

            assert len(resolved) == 1, (
                f"Non-eligible function must not expand — expected 1 check, got {len(resolved)}. "
                f"is_multiselect_eligible() must gate expansion in register_from_config."
            )
        finally:
            BUILT_IN_CHECKS.pop("no_params_check", None)

    def test_non_series_return_not_expanded(self):
        """Function without pd.Series or pd.DataFrame return must not expand.

        is_expandable() in CheckParamInspector gates alias_map expansion:
        only functions returning pd.Series or pd.DataFrame are expanded.
        Dict-returning functions must always register as a single check
        regardless of alias_map contents.
        """
        from proto_pipe.io.registry import register_from_config
        from proto_pipe.checks.built_in import BUILT_IN_CHECKS

        BUILT_IN_CHECKS["dict_return_check"] = check_dict_return
        try:
            reg, rep_reg = CheckRegistry(), ReportRegistry()
            config = {
                "templates": {},
                "reports": [{
                    "name": "test_report",
                    "source": {"type": "duckdb", "path": "", "table": "t"},
                    "alias_map": [
                        {"param": "col", "column": "A"},
                        {"param": "col", "column": "B"},
                    ],
                    "options": {"parallel": False},
                    "checks": [{"name": "dict_return_check", "params": {"col": "price"}}],
                }],
            }
            register_from_config(config, reg, rep_reg)
            resolved = rep_reg.get("test_report")["resolved_checks"]

            assert len(resolved) == 1, (
                "Function without pd.Series[bool] return must not be expanded by alias_map"
            )
        finally:
            BUILT_IN_CHECKS.pop("dict_return_check", None)


# ===========================================================================
# CheckParamInspector.is_expandable() — behavioral guarantees (rule 9)
# ===========================================================================

class TestIsExpandable:
    """Behavioral guarantees for CheckParamInspector.is_expandable().

    is_expandable() is the alias_map expansion gate in _expand_check_with_alias_map.
    True for pd.Series and pd.DataFrame returns; False for dict, unannotated, int etc.
    """

    def test_series_bool_return_is_expandable(self):
        """pd.Series[bool]-returning function (check) is expandable."""
        def check(col: str) -> "pd.Series[bool]":
            pass
        assert CheckParamInspector(check).is_expandable() is True

    def test_series_return_is_expandable(self):
        """pd.Series-returning function (transform) is expandable."""
        def transform(col: str) -> "pd.Series":
            pass
        assert CheckParamInspector(transform).is_expandable() is True

    def test_dataframe_return_is_expandable(self):
        """pd.DataFrame-returning function (transform) is expandable."""
        def transform(df: "pd.DataFrame") -> "pd.DataFrame":
            pass
        assert CheckParamInspector(transform).is_expandable() is True

    def test_dict_return_is_not_expandable(self):
        """dict-returning function must not be expandable — per-column semantics undefined."""
        def bad_check(col: str) -> dict:
            pass
        assert CheckParamInspector(bad_check).is_expandable() is False

    def test_int_return_is_not_expandable(self):
        """int-returning function is not expandable."""
        def scalar_func(col: str) -> int:
            pass
        assert CheckParamInspector(scalar_func).is_expandable() is False

    def test_unannotated_return_is_not_expandable(self):
        """Function with no return annotation is not expandable."""
        def unannotated(col: str):
            pass
        assert CheckParamInspector(unannotated).is_expandable() is False

    def test_expand_gate_blocks_dict_return(self):
        """_expand_check_with_alias_map registers single check for dict-returning function.

        Integration guarantee: even when alias_map has entries, a dict-returning
        function must never produce more than one resolved check.
        """
        from proto_pipe.io.registry import _expand_check_with_alias_map, _build_alias_param_map
        from proto_pipe.checks.built_in import BUILT_IN_CHECKS

        def dict_func(col: str) -> dict:
            pass

        BUILT_IN_CHECKS["_test_dict_func"] = dict_func
        try:
            reg = CheckRegistry()
            reg.register("_test_dict_func", dict_func)
            names = _expand_check_with_alias_map(
                func_name="_test_dict_func",
                params={},
                alias_param_map={"col": ["A", "B", "C"]},
                check_registry=reg,
            )
            assert len(names) == 1, (
                f"dict-returning function must not be expanded by alias_map, got {len(names)}"
            )
        finally:
            BUILT_IN_CHECKS.pop("_test_dict_func", None)

    def test_expand_gate_allows_series_return(self):
        """_expand_check_with_alias_map expands pd.Series[bool]-returning function.

        Integration guarantee: a check with pd.Series[bool] return and N alias_map
        entries produces N resolved checks.
        """
        from proto_pipe.io.registry import _expand_check_with_alias_map
        from proto_pipe.checks.built_in import BUILT_IN_CHECKS

        def series_check(col: str) -> "pd.Series[bool]":
            pass

        BUILT_IN_CHECKS["_test_series_check"] = series_check
        try:
            reg = CheckRegistry()
            reg.register("_test_series_check", series_check)
            names = _expand_check_with_alias_map(
                func_name="_test_series_check",
                params={},
                alias_param_map={"col": ["A", "B"]},
                check_registry=reg,
            )
            assert len(names) == 2, (
                f"pd.Series[bool]-returning check must expand to 2, got {len(names)}"
            )
        finally:
            BUILT_IN_CHECKS.pop("_test_series_check", None)
