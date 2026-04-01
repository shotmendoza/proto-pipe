# Adding a New Check

Checks are functions that inspect a DataFrame column (or the full table) and
return a `pd.Series[bool]` — one value per row, `True` = pass, `False` = fail.
Transforms return a `pd.Series` or `pd.DataFrame` that gets written back to
the table. Both are registered with `@custom_check`.

---

## What a check function looks like

```python
import pandas as pd
from proto_pipe.checks.helpers import custom_check

@custom_check("null_premium_check", kind="check")
def null_premium_check(col: str) -> pd.Series:
    """Check that premium values are not null.

    Returns True for rows that pass, False for rows that fail.
    """
    def _run(context: dict) -> pd.Series:
        df = context["df"]
        return df[col].notna()
    return _run
```

- `col: str` — annotated as `str` tells the wizard this is a **column selector**.
  The user picks the column from a list when running `vp new report`.
- Return type `-> pd.Series` is required for `kind='check'`.
  `pd.Series[bool]` is also accepted and more explicit.
- `context["df"]` is the watermark-filtered DataFrame for this report run.

---

## Check kinds

| Kind        | Return type                   | What happens                                                                |
|-------------|-------------------------------|-----------------------------------------------------------------------------|
| `check`     | `pd.Series[bool]`             | Failures written to `validation_flags`. Warns, does not block deliverables. |
| `transform` | `pd.Series` or `pd.DataFrame` | Written back to the DuckDB table after all checks run.                      |

---

## Parameter types

The `vp new report` wizard reads your function signature to build the
configuration prompt. Parameter annotations control what kind of prompt
the user sees:

| Annotation                     | Behaviour                                                                                                 |
|--------------------------------|-----------------------------------------------------------------------------------------------------------|
| `col: str` or `col: pd.Series` | Column selector — user picks from available table columns. Supports multi-select for alias_map expansion. |
| Unannotated                    | Treated as a column selector by convention.                                                               |
| `threshold: float`             | Scalar — free-text prompt. Broadcast across all column runs.                                              |
| `df: pd.DataFrame`             | Auto-filled with the full table — never prompted.                                                         |

---

## Registering your checks

Point `custom_checks_module` in `pipeline.yaml` at your checks file:

```yaml
custom_checks_module: "my_checks.py"
```

The pipeline loads your module at startup and registers all `@custom_check`
decorated functions automatically.

Run `vp funcs` to confirm everything registered correctly:

```
── Check Functions ─────────────────────────

Built-in checks (4):
  ✓  null_check  [check]
  ✓  range_check  [check]
  ...

Custom checks (1):
  ✓  null_premium_check  [check]

Summary: 5 passed
```

Checks marked `✗` are silently skipped during `vp new report`. Fix the issue
shown and run `vp funcs` again.

---

## Multi-column checks with alias_map

When a check has a `str` or `pd.Series` column param, the `vp new report`
wizard offers a checkbox to run the check once per selected column. This
creates one alias_map entry per column at config time and expands to one
registered check instance per column at runtime.

```python
@custom_check("value_not_negative", kind="check")
def value_not_negative(col: str) -> pd.Series:
    """Check that values in a column are not negative."""
    def _run(context: dict) -> pd.Series:
        df = context["df"]
        return df[col] >= 0
    return _run
```

Selecting `premium` and `renewal` runs two instances:
one checking `premium >= 0`, one checking `renewal >= 0`.

---

## Transform example

Transforms return a Series or DataFrame that replaces column data in the table.

```python
@custom_check("normalize_region", kind="transform")
def normalize_region(col: str) -> pd.Series:
    """Uppercase and strip the region column."""
    def _run(context: dict) -> pd.Series:
        df = context["df"]
        result = df[col].str.upper().str.strip()
        result.name = col
        return result
    return _run
```

The series `.name` must match the column to overwrite.

---

## Built-in checks reference

| Name              | Params                                         | Description                           |
|-------------------|------------------------------------------------|---------------------------------------|
| `null_check`      | `col: str`                                     | Flags rows where the column is null   |
| `range_check`     | `col: str`, `min_val: float`, `max_val: float` | Flags rows outside the declared range |
| `schema_check`    | `expected_cols: list`                          | Flags missing expected columns        |
| `duplicate_check` | `subset: list` (optional)                      | Flags duplicate rows                  |

Run `vp funcs` to see the full list at any time.