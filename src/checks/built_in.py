"""
Built-in check functions.

Each check accepts a context dict with a 'df' key (pandas DataFrame)
plus any additional params baked in via partial at registration time.
"""


def check_nulls(
        context: dict
) -> dict:
    """Check for null values across all columns.

    :param context: a dictionary with `df` as a key and a DataFrame as value
    """
    null_counts = context["df"].isnull().sum()
    return {
        "null_counts": null_counts.to_dict(),
        "has_nulls": bool(null_counts.any()),
    }


def check_range(
        context: dict,
        col: str,
        min_val: float,
        max_val: float
) -> dict:
    """Check that a column's values fall within [min_val, max_val].

    :param context: a dictionary with `df` as a key and a DataFrame as value
    :param col: the column to check
    :param min_val: the minimum allowed value
    :param max_val: the maximum allowed value
    """
    df = context["df"]
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found in DataFrame")
    out_of_range = df[~df[col].between(min_val, max_val)]
    return {
        "col": col,
        "min_val": min_val,
        "max_val": max_val,
        "violations": len(out_of_range),
        "violation_indices": out_of_range.index.tolist(),
    }


def check_schema(
        context: dict,
        expected_cols: list[str]
) -> dict:
    """Check that the DataFrame has the expected columns."""
    actual = set(context["df"].columns)
    expected = set(expected_cols)
    return {
        "missing_cols": list(expected - actual),
        "extra_cols": list(actual - expected),
        "matches": expected == actual,
    }


def check_duplicates(
        context: dict,
        subset: list[str] | None = None
) -> dict:
    """Check for duplicate rows, optionally scoped to a subset of columns."""
    df = context["df"]
    dupes = df.duplicated(subset=subset)
    return {
        "duplicate_count": int(dupes.sum()),
        "has_duplicates": bool(dupes.any()),
        "subset": subset,
    }


# Map of function name -> function, used when loading checks from config
BUILT_IN_CHECKS = {
    "null_check": check_nulls,
    "range_check": check_range,
    "schema_check": check_schema,
    "duplicate_check": check_duplicates,
}
