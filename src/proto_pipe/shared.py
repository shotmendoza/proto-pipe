"""Shared utilities used across proto_pipe subpackages.

Annotation parsing helpers live here because both checks/ and macros/
need them. No cross-imports between those packages — shared.py is the
common dependency.
"""
from __future__ import annotations

import inspect


def is_str_annotation(ann) -> bool:
    """True for str type or 'str' string annotation.

    Handles both evaluated annotations (ann is str) and string annotations
    produced by `from __future__ import annotations` (ann == "str").
    """
    return ann is str or ann == "str"


def is_series_annotation(ann) -> bool:
    """True if annotation refers to pd.Series (column selector, not DataFrame).

    pd.Series params are column selectors — the runner extracts the named
    column as a Series and passes it directly to the function. They behave
    identically to str params in the prompt flow (alias_map + column picker)
    but the function receives the actual Series rather than the column name.

    Handles both evaluated annotations (pd.Series, pd.Series[bool]) and
    string annotations produced by `from __future__ import annotations`.
    Explicitly excludes DataFrame annotations.
    """
    if ann is inspect.Parameter.empty:
        return False
    ann_str = str(ann) if not isinstance(ann, str) else ann
    return "Series" in ann_str and "DataFrame" not in ann_str


def is_dataframe_annotation(ann) -> bool:
    """True if annotation refers to pd.DataFrame."""
    if ann is inspect.Parameter.empty:
        return False
    ann_str = str(ann) if not isinstance(ann, str) else ann
    return "DataFrame" in ann_str
