"""
Built-in check functions.

Each check returns pd.Series[bool] — True where a row PASSES, False where it FAILS.
Conversion to CheckResult is handled by the registry wrapper, not here.
"""
import pandas as pd


def check_nulls(context: dict) -> pd.Series:
    """True where a row has no nulls, False where any column is null."""
    df = context["df"]
    return ~df.isnull().any(axis=1)


def check_range(
        context: dict,
        col: str,
        min_val: float,
        max_val: float,
) -> pd.Series:
    """True where col is within [min_val, max_val], False otherwise."""
    df = context["df"]
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found in DataFrame")
    return df[col].between(min_val, max_val)


def check_schema(
        context: dict,
        expected_cols: list[str],
) -> pd.Series:
    """True for all rows if schema matches exactly, False for all rows otherwise."""
    df = context["df"]
    actual = set(df.columns)
    expected = set(expected_cols)
    missing = expected - actual
    extra = actual - expected
    passing = len(missing) == 0 and len(extra) == 0
    return pd.Series([passing] * len(df), index=df.index)


def check_duplicates(
        context: dict,
        subset: list[str] | None = None,
) -> pd.Series:
    """True where row is not a duplicate, False where it is."""
    df = context["df"]
    return ~df.duplicated(subset=subset, keep="first")


BUILT_IN_CHECKS = {
    "null_check": check_nulls,
    "range_check": check_range,
    "schema_check": check_schema,
    "duplicate_check": check_duplicates,
}
