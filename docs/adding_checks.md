# Adding a New Check

There are two kinds of functions you can register: **checks** and **transforms**.

- **Checks** (`kind="check"`) inspect a DataFrame and return `pd.Series[bool]`
  — `True` where a row passes, `False` where it fails. Failures are written
  to `validation_flags` for review.
- **Transforms** (`kind="transform"`) modify data in place. They run after
  all checks and write their result back to the report table in DuckDB.
  See [Transform checks](#transform-checks) below.

---

## What a check function looks like

```python
def my_check(context: dict, col: str = "price") -> pd.Series:
    df = context["df"]
    return df[col] >= 0   # True = row passes, False = row fails
```

- `context["df"]` is a pandas DataFrame of the rows being validated.
- The function must return `pd.Series[bool]` — `True` where a row passes.
- Named parameters (other than `context`) are column selectors or scalar
  thresholds. Column params must be annotated `str`.
- **Pass vs fail:** rows where the Series is `False` are flagged in
  `validation_flags`, keyed on the record's primary key value.

---

## Option 1 — The `@custom_check` decorator (recommended)

Decorate your function with `@custom_check("check_name")`, save it in a
module, and point the pipeline at that module via `custom_checks_module` in
`pipeline.yaml`. All decorated functions are registered automatically on startup.

**Step 1 — Write the check function:**

```python
# my_checks.py

import pandas as pd
from proto_pipe.checks.helpers import custom_check


@custom_check("margin_check")
def check_margin(context: dict, col: str = "margin", threshold: float = 0.2) -> pd.Series:
    df = context["df"]
    return df[col] >= threshold   # True = passes, False = below threshold
```

**Step 2 — Register the module in `pipeline.yaml`:**

```yaml
custom_checks_module: "my_checks.py"
```

That's it. On the next run, `margin_check` is available by name in any report.

---

## Transform checks

Transforms modify column values rather than flagging rows. They run after
all checks complete and write their result back to the report table in DuckDB.

```python
@custom_check("normalize_transaction_type", kind="transform")
def transform_transaction_type(context: dict, col: str = "transaction_type") -> pd.Series:
    df = context["df"]
    result = df[col].replace({"Issuance": "Reinstatement"})
    result.name = col   # name must match the column to update
    return result
```

Rules:
- Return a `pd.Series` with `.name` set to the column you want to update,
  or a `pd.DataFrame` to replace the entire table.
- Transforms run in config order — the output of one is the input to the next.
- On exception, that transform is skipped and the table is left unchanged.
  The error is reported but does not stop subsequent transforms.

---

## Option 2 — `register_custom_check` in code

Use this when you need to bake in default params at registration time, or
when you're building checks programmatically rather than from a module file.

```python
import pandas as pd
from proto_pipe.checks.helpers import register_custom_check
from proto_pipe.checks.registry import check_registry


def check_margin(context: dict, col: str = "margin", threshold: float = 0.2) -> pd.Series:
    df = context["df"]
    return df[col] >= threshold


register_custom_check("margin_check", check_margin, check_registry)
```

Call `register_custom_check` before `load_config` / `register_from_config`
so the config loader can find the function by name.

---

## Option 3 — Inline check in config (no code needed)

If the built-ins already cover your case, add a check directly to the
report in `reports_config.yaml`:

```yaml
reports:
  - name: "my_report"
    source:
      table: "sales"
      timestamp_col: "updated_at"
    checks:
      - name: range_check
        params:
          col: "discount"
          min_val: 0
          max_val: 1
```

No code changes, no reinstall. The pipeline picks it up on the next run.

---

## Referencing a custom check from config

Once registered (via decorator or `register_custom_check`), reference it in
`reports_config.yaml` exactly like a built-in:

```yaml
checks:
  - name: margin_check
    params:
      threshold: 0.15
      # col is resolved from alias_map if defined on the report
```

Or define it as a template for reuse across multiple reports:

```yaml
templates:
  margin_threshold_check:
    name: margin_check
    params:
      threshold: 0.15
```

---

## Alias map — running the same check across multiple columns

When the same check needs to run on multiple columns in the same report
(e.g. Coverage A, Coverage B, Coverage C), use `alias_map` on the report
instead of repeating the check with different params.

```yaml
reports:
  - name: "us_carrier"
    source:
      table: "us_carrier"
      timestamp_col: "updated_at"
    alias_map:
      - param: col
        column: "Coverage A"
      - param: col
        column: "Coverage B"
      - param: col
        column: "Coverage C"
    checks:
      - name: range_check
        params:
          min_val: 0
          max_val: 1000000
          # col resolved from alias_map — runs once per entry
```

The check runs once per `alias_map` entry for the matching param. Scalar
params (`min_val`, `max_val`) broadcast across all runs. Single-entry params
also broadcast silently.

Use `vp new-report` to set up `alias_map` interactively — the wizard prompts
you to select columns for each param and builds the config automatically.

---

## Built-in checks reference

| Name               | Params                                          | What it checks              |
|--------------------|-------------------------------------------------|-----------------------------|
| `null_check`       | none                                            | No nulls in any column      |
| `range_check`      | `col: str`, `min_val: float`, `max_val: float`  | Value within range          |
| `schema_check`     | `expected_cols: list[str]`                      | Exact column set matches    |
| `duplicate_check`  | `subset: list[str]` (optional)                  | No duplicate rows           |

---

## SQL macros

Scalar SQL expressions used inside deliverable or view SQL files can be
defined once as macros and reused across any carrier.

```bash
vp new-macro normalize_transaction_type
```

This creates a template `.sql` file in your `macros` directory:

```sql
-- normalize_transaction_type.sql
CREATE OR REPLACE MACRO normalize_transaction_type(val) AS
    CASE
        WHEN val = 'Issuance' THEN 'Reinstatement'
        ELSE val
    END;
```

Macros are registered at pipeline startup and available in any SQL query:

```sql
SELECT normalize_transaction_type(transaction_type) AS transaction_type
FROM sales
```

---

## Seeing registered checks

```bash
vp checks
```

Lists all built-ins and custom checks registered via decorator or
`register_custom_check`.

---

## Tips

- Keep check functions pure — read `context["df"]`, return a Series.
  Don't write to disk or mutate shared state inside a check.
- Checks run per-report, not per-row. The DataFrame is the full slice of
  new rows for that report (watermark-filtered).
- If `parallel: true` is set on the report, checks run in threads.
  Make sure your check function is thread-safe.
- Validation flags are warnings — they do not block a deliverable from being
  produced. Use `vp export-validation` to review them after `vp validate`.
- If a report table has been modified by transforms and you need to start
  fresh, use `vp table-reset --report <name>` to drop the table and
  re-ingest from source files.