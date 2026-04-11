"""Unit tests for macros/registry.py.

Tests: MacroContract, MacroParamInspector, validate_macro,
parse_macro_signature, MacroRegistry.
"""
from __future__ import annotations

import pandas as pd
import pytest

from proto_pipe.macros.registry import (
    MacroContract,
    MacroParamInspector,
    MacroRegistry,
    parse_macro_signature,
    validate_macro,
)


# ---------------------------------------------------------------------------
# validate_macro
# ---------------------------------------------------------------------------

class TestValidateMacro:

    def test_accepts_valid_scalar_function(self):
        def apply_share(premium: float, share: float) -> float:
            return premium * share

        contract = validate_macro(apply_share)

        assert isinstance(contract, MacroContract)
        assert contract.name == "apply_share"
        assert contract.params == ["premium", "share"]
        assert contract.param_types == {"premium": float, "share": float}
        assert contract.return_type is float
        assert contract.source == "python"
        assert contract.func is apply_share

    def test_accepts_unannotated_function(self):
        def simple(x, y):
            return x + y

        contract = validate_macro(simple)

        assert contract.params == ["x", "y"]
        assert contract.param_types == {}
        assert contract.return_type is None

    def test_rejects_series_param(self):
        def bad(col: pd.Series) -> pd.Series:
            return col * 2

        with pytest.raises(TypeError, match="Series"):
            validate_macro(bad)

    def test_rejects_dataframe_param(self):
        def bad(df: pd.DataFrame) -> float:
            return df["a"].sum()

        with pytest.raises(TypeError, match="DataFrame"):
            validate_macro(bad)

    def test_rejects_series_return(self):
        def bad(x: float) -> "pd.Series":
            pass

        with pytest.raises(TypeError, match="Series"):
            validate_macro(bad)

    def test_rejects_dataframe_return(self):
        def bad(x: float) -> "pd.DataFrame":
            pass

        with pytest.raises(TypeError, match="DataFrame"):
            validate_macro(bad)

    def test_rejects_non_callable(self):
        with pytest.raises(TypeError, match="callable"):
            validate_macro("not a function")

    def test_rejects_context_dict_param(self):
        def bad(context: dict) -> float:
            return 0.0

        with pytest.raises(TypeError, match="context"):
            validate_macro(bad)

    def test_accepts_mixed_annotated_unannotated(self):
        def mixed(a: int, b, c: str) -> str:
            return f"{a}{b}{c}"

        contract = validate_macro(mixed)

        assert contract.params == ["a", "b", "c"]
        assert contract.param_types == {"a": int, "c": str}
        assert contract.return_type is str


# ---------------------------------------------------------------------------
# parse_macro_signature
# ---------------------------------------------------------------------------

class TestParseMacroSignature:

    def test_parses_create_or_replace(self):
        sql = "CREATE OR REPLACE MACRO normalize(val) AS CASE WHEN val = 'X' THEN 'Y' ELSE val END;"
        result = parse_macro_signature(sql)
        assert result == ("normalize", ["val"])

    def test_parses_create_macro(self):
        sql = "CREATE MACRO add_one(x) AS x + 1;"
        result = parse_macro_signature(sql)
        assert result == ("add_one", ["x"])

    def test_parses_multiple_params(self):
        sql = "CREATE OR REPLACE MACRO apply_share(premium, share) AS premium * share;"
        result = parse_macro_signature(sql)
        assert result == ("apply_share", ["premium", "share"])

    def test_parses_no_params(self):
        sql = "CREATE MACRO get_pi() AS 3.14159;"
        result = parse_macro_signature(sql)
        assert result == ("get_pi", [])

    def test_returns_none_for_invalid_sql(self):
        assert parse_macro_signature("SELECT 1") is None

    def test_returns_none_for_empty_string(self):
        assert parse_macro_signature("") is None

    def test_case_insensitive(self):
        sql = "create or replace macro lower_case(x) as x;"
        result = parse_macro_signature(sql)
        assert result == ("lower_case", ["x"])


# ---------------------------------------------------------------------------
# MacroRegistry
# ---------------------------------------------------------------------------

class TestMacroRegistry:

    def test_register_and_available(self):
        reg = MacroRegistry()
        contract = MacroContract(
            name="double_val",
            params=["x"],
            param_types={"x": float},
            return_type=float,
            source="python",
            func=lambda x: x * 2,
            func_name="double_val",
        )
        reg.register(contract)

        assert reg.available() == ["double_val"]

    def test_get_contract(self):
        reg = MacroRegistry()
        contract = MacroContract(
            name="add", params=["a", "b"], param_types={},
            return_type=None, source="sql",
        )
        reg.register(contract)

        assert reg.get_contract("add") is contract
        assert reg.get_contract("missing") is None

    def test_available_sorted(self):
        reg = MacroRegistry()
        for name in ["zebra", "alpha", "mid"]:
            reg.register(MacroContract(
                name=name, params=[], param_types={},
                return_type=None, source="sql",
            ))

        assert reg.available() == ["alpha", "mid", "zebra"]

    def test_register_overwrites(self):
        reg = MacroRegistry()
        c1 = MacroContract(name="f", params=["x"], param_types={},
                           return_type=None, source="sql")
        c2 = MacroContract(name="f", params=["x", "y"], param_types={},
                           return_type=None, source="python")
        reg.register(c1)
        reg.register(c2)

        assert reg.get_contract("f").source == "python"
        assert reg.get_contract("f").params == ["x", "y"]


# ---------------------------------------------------------------------------
# MacroParamInspector
# ---------------------------------------------------------------------------

class TestMacroParamInspector:

    def test_param_names(self):
        def f(a: int, b: str, c: float) -> float:
            pass

        inspector = MacroParamInspector(f)
        assert inspector.param_names() == ["a", "b", "c"]

    def test_param_types(self):
        def f(a: int, b, c: str) -> float:
            pass

        inspector = MacroParamInspector(f)
        assert inspector.param_types() == {"a": int, "c": str}

    def test_return_type(self):
        def f(x: float) -> bool:
            pass

        assert MacroParamInspector(f).return_type() is bool

    def test_return_type_none_when_absent(self):
        def f(x):
            pass

        assert MacroParamInspector(f).return_type() is None

    def test_string_annotations_resolved(self):
        """String annotations from `from __future__ import annotations` resolve."""
        def f(x: int, y: float) -> str:
            pass

        inspector = MacroParamInspector(f)
        assert inspector.param_types() == {"x": int, "y": float}
        assert inspector.return_type() is str
