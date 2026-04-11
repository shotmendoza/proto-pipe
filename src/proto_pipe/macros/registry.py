"""Macro registry — stores validated MacroContract objects.

Macros are a deliverable-layer concern: they run inside SQL queries,
not during vp validate. Separate from checks/ by design (see CLAUDE.md).
"""
from __future__ import annotations

import inspect
import re

from dataclasses import dataclass
from typing import Callable, Literal

from proto_pipe.shared import is_series_annotation, is_dataframe_annotation


@dataclass(frozen=True)
class MacroContract:
    """Validated macro definition — produced by validate_macro(), never mutated."""

    name: str
    """Macro name (used in SQL calls)."""

    params: list[str]
    """Ordered param names."""

    param_types: dict[str, type]
    """param_name → Python type. Empty dict for SQL macros (untyped)."""

    return_type: type | None
    """Python return type; None for SQL macros."""

    source: Literal["python", "sql"]
    """Origin — determines registration method."""

    func: Callable | None = None
    """Python callable; None for SQL-only macros."""

    func_name: str = ""
    """Original __name__ for display in wizard."""


class MacroParamInspector:
    """Inspect a Python macro function's signature.

    Simpler than CheckParamInspector — macros are always scalar.
    No Series/DataFrame routing, no alias_map, no kind routing.
    """

    def __init__(self, func: Callable):
        self._sig = inspect.signature(func)

    def param_names(self) -> list[str]:
        """Ordered parameter names."""
        return list(self._sig.parameters.keys())

    def param_types(self) -> dict[str, type]:
        """param_name → type for annotated params. Unannotated params omitted."""
        result = {}
        for name, param in self._sig.parameters.items():
            if param.annotation is not inspect.Parameter.empty:
                ann = param.annotation
                # Resolve string annotations to actual types where possible
                if isinstance(ann, str):
                    _type = _resolve_str_annotation(ann)
                    if _type is not None:
                        result[name] = _type
                else:
                    result[name] = ann
        return result

    def return_type(self) -> type | None:
        """Return type annotation, or None if absent."""
        ret = self._sig.return_annotation
        if ret is inspect.Parameter.empty:
            return None
        if isinstance(ret, str):
            return _resolve_str_annotation(ret)
        return ret


def _resolve_str_annotation(ann_str: str) -> type | None:
    """Resolve common string annotations to Python types."""
    mapping = {"int": int, "float": float, "str": str, "bool": bool}
    return mapping.get(ann_str)


def validate_macro(func: Callable) -> MacroContract:
    """Validate a Python function as a macro. Returns MacroContract or raises.

    Validation rules:
    1. Must be callable
    2. All params must have scalar annotations (int, float, str, bool) or be unannotated
    3. No pd.Series, pd.DataFrame, or context: dict params
    4. Return type must be scalar or unannotated
    5. Function must be importable and not raise on introspection
    """
    if not callable(func):
        raise TypeError(f"Macro must be callable, got {type(func).__name__}")

    try:
        sig = inspect.signature(func)
    except (ValueError, TypeError) as e:
        raise TypeError(f"Cannot inspect macro '{getattr(func, '__name__', '?')}': {e}")

    # Check each param
    for name, param in sig.parameters.items():
        ann = param.annotation
        if ann is inspect.Parameter.empty:
            continue  # unannotated is fine

        if is_series_annotation(ann):
            raise TypeError(
                f"Macro param '{name}' has Series annotation. "
                "Macros must be scalar functions. Use @custom_check for Series/DataFrame operations."
            )
        if is_dataframe_annotation(ann):
            raise TypeError(
                f"Macro param '{name}' has DataFrame annotation. "
                "Macros must be scalar functions. Use @custom_check for Series/DataFrame operations."
            )

        # Reject context: dict pattern
        if name == "context":
            ann_str = str(ann) if not isinstance(ann, str) else ann
            if "dict" in ann_str.lower():
                raise TypeError(
                    f"Macro param 'context' has dict annotation. "
                    "Macros are scalar — they don't receive a context dict."
                )

    # Check return type
    ret = sig.return_annotation
    if ret is not inspect.Parameter.empty:
        if is_series_annotation(ret):
            raise TypeError(
                "Macro return type is Series. Macros must return scalar values."
            )
        if is_dataframe_annotation(ret):
            raise TypeError(
                "Macro return type is DataFrame. Macros must return scalar values."
            )

    inspector = MacroParamInspector(func)

    return MacroContract(
        name=getattr(func, "__name__", "unknown"),
        params=inspector.param_names(),
        param_types=inspector.param_types(),
        return_type=inspector.return_type(),
        source="python",
        func=func,
        func_name=getattr(func, "__name__", ""),
    )


def parse_macro_signature(sql_text: str) -> tuple[str, list[str]] | None:
    """Parse 'CREATE [OR REPLACE] MACRO name(a, b) AS ...' → ('name', ['a', 'b']).

    Returns None if no valid macro signature found.
    """
    match = re.search(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?MACRO\s+(\w+)\(([^)]*)\)",
        sql_text,
        re.IGNORECASE,
    )
    if not match:
        return None

    name = match.group(1)
    params_str = match.group(2).strip()
    params = [p.strip() for p in params_str.split(",") if p.strip()] if params_str else []
    return (name, params)


class MacroRegistry:
    """Registry of validated MacroContract objects, keyed by name."""

    def __init__(self):
        self._macros: dict[str, MacroContract] = {}

    def register(self, contract: MacroContract) -> None:
        """Register a validated MacroContract."""
        self._macros[contract.name] = contract

    def available(self) -> list[str]:
        """Return sorted list of registered macro names."""
        return sorted(self._macros.keys())

    def get_contract(self, name: str) -> MacroContract | None:
        """Return MacroContract for the given name, or None."""
        return self._macros.get(name)