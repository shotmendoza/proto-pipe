# Adding a New Check

Checks are functions that inspect a DataFrame and return a dict.
There are two ways to add one: as a **custom check** (registered in code
before the pipeline starts) or as a **config-only check** (an inline check
in `reports_config.yaml` that references an existing check by name).

---

## What a check function looks like

```python
def my_check(context: dict, **params) -> dict:
    df = context["df"]
    # ... your logic ...
    return {"some_key": some_value}
```

- `context["df"]` is a pandas DataFrame of the rows being validated.
- Any extra params are passed as keyword arguments at registration time.
- **Return value:** any dict. The pipeline does not enforce a schema on it.
- **Pass vs fail:** a check is marked `passed` if it returns normally,
  `failed` if it raises an exception. Raise if something is wrong:

```python
def check_no_negatives(context: dict, col: str) -> dict:
    df = context["df"]
    neg = df[df[col] < 0]
    if not neg.empty:
        raise ValueError(f"{len(neg)} negative values found in '{col}'")
    return {"col": col, "status": "ok"}
```

---

## Row-level flagging

The pipeline inspects each check's return dict to decide how to write
validation flags. The return shape determines whether flags are row-level
(one flag per bad record) or a single summary flag for the whole check run.

**Boolean mask** — return a `pd.Series[bool]` under the key `"mask"`, and
set `"flag_when"` to indicate which boolean value means a row failed.
`flag_when` defaults to `True` if omitted.

```python
# Flag rows where price is negative
def check_no_negatives(context, col="price"):
    df = context["df"]
    return {
        "mask": df[col] < 0,   # True where the row is bad
        "flag_when": True,      # flag rows where mask == True (default, can omit)
    }

# Flag rows where is_active is False
def check_all_active(context):
    df = context["df"]
    return {
        "mask": df["is_active"],
        "flag_when": False,     # flag rows where mask == False
    }
```

Each flagged row gets its own entry in `validation_flags`, keyed on the
record's primary key value. The `vp export-validation` Detail sheet will
show one row per flagged record with the check name and reason.

**Summary** — if the return dict contains neither `"mask"` nor
`"violation_indices"`, one summary flag is written for the entire check run.
This is how the built-in `null_check`, `schema_check`, and `duplicate_check`
work today — they report an aggregate result rather than individual rows.

---

## Option 1 — The `@custom_check` decorator (recommended)

The cleanest way to register a custom check. Decorate your function with
`@custom_check("check_name")`, save it in a module, and point the pipeline
at that module via `custom_checks_module` in `pipeline.yaml`. The pipeline
registers all decorated functions automatically on startup.

**Step 1 — Write the check function:**

```python
# my_checks.py

from validation_pipeline import custom_check


@custom_check("margin_check")
def check_margin(context: dict, col: str = "margin", threshold: float = 0.2) -> dict:
    df = context["df"]
    below = df[df[col] < threshold]
    if not below.empty:
        raise ValueError(f"{len(below)} rows below margin threshold {threshold}")
    return {"violations": 0, "threshold": threshold}
```

**Step 2 — Register the module in `pipeline.yaml`:**

```yaml
custom_checks_module: "my_checks.py"
```

That's it. On the next run, `margin_check` is available by name in any report.

---

## Option 2 — `register_custom_check` in code

Use this when you need to bake in default params at registration time, or
when you're building checks programmatically rather than from a module file.

```python
# my_checks.py (or anywhere that runs before the pipeline starts)

from validation_pipeline import custom_check, register_custom_check, check_registry


def check_margin(context: dict, col: str = "margin", threshold: float = 0.2) -> dict:
    df = context["df"]
    below = df[df[col] < threshold]
    if not below.empty:
        raise ValueError(f"{len(below)} rows below margin threshold {threshold}")
    return {"violations": 0, "threshold": threshold}


register_custom_check("margin_check", check_margin, check_registry)
```

Call `register_custom_check` **before** `load_config` / `register_from_config`
so the config loader can find the function by name.

> **Note:** Do not use `check_registry.register()` directly for checks you
> want to reference from `reports_config.yaml`. The config loader resolves
> check names through the internal `BUILT_IN_CHECKS` registry, which only
> `register_custom_check` and `@custom_check` update. Using
> `check_registry.register()` alone will cause a `ValueError` at config
> load time.

---

## Option 3 — Inline check in config (no code needed)

If the built-ins already cover your case, just add a check directly to the
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
      col: "margin"
      threshold: 0.15
```

Or define it as a template for reuse across multiple reports:

```yaml
templates:
  margin_threshold_check:
    name: margin_check
    params:
      col: "margin"
      threshold: 0.15
```

---

## Built-in checks reference

| Name               | Params                                          | Raises when...              | Flag type   |
|--------------------|-------------------------------------------------|-----------------------------|-------------|
| `null_check`       | none                                            | never                       | summary     |
| `range_check`      | `col`, `min_val`, `max_val`                     | column not found            | row-level   |
| `schema_check`     | `expected_cols` (list)                          | never                       | summary     |
| `duplicate_check`  | `subset` (optional list)                        | never                       | summary     |

> **Note:** Built-ins that are marked "summary" report an aggregate result
> without identifying specific rows. If you need row-level flags, write a
> custom check that returns a `"mask"` key.

---

## Seeing registered checks

```bash
vp checks
```

This lists all built-ins. Custom checks registered via decorator or
`register_custom_check` will also appear here after registration.

---

## Tips

- Keep check functions pure — read `context["df"]`, return a dict, raise on
  failure. Don't write to disk or mutate shared state inside a check.
- Checks run per-report, not per-row. The DataFrame passed in is the full
  slice of new rows for that report (watermark-filtered).
- If `parallel: true` is set on the report, checks run in threads.
  Make sure your check function is thread-safe (no shared mutable state).
- Validation flags are warnings — they do not block a deliverable from being
  produced. Use `vp export-validation` to review them after `vp validate`.
- 