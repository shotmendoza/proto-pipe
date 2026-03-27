# Adding a New Check

Checks are functions that inspect a DataFrame and return a dict.
There are two ways to add one: as a **built-in** (registered in code before
the pipeline starts) or as a **config-only check** (an inline check in
`reports_config.yaml` that references an existing built-in by name).

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
  `failed` if it raises an exception. So raise if something is wrong:

```python
def check_no_negatives(context: dict, col: str) -> dict:
    df = context["df"]
    neg = df[df[col] < 0]
    if not neg.empty:
        raise ValueError(f"{len(neg)} negative values found in '{col}'")
    return {"col": col, "status": "ok"}
```

---

## Option 1 — Register a custom check in code

Do this when your check logic is specific to your project and not
expressible with the built-ins.

```python
# my_checks.py (or anywhere that runs before the pipeline starts)

from validation_pipeline import check_registry
from validation_pipeline.checks.registry_helpers import register_custom_check


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

Once registered, reference it in `reports_config.yaml` like any built-in:

```yaml
checks:
  - name: margin_check
    params:
      col: "margin"
      threshold: 0.15
```

Or define it as a template:

```yaml
templates:
  margin_threshold_check:
    name: margin_check
    params:
      col: "margin"
      threshold: 0.15
```

---

## Option 2 — Inline check in config (no code needed)

If the built-ins already cover your case, just add an inline check directly
to the report in `reports_config.yaml`:

```yaml
reports:
  - name: "my_report"
    source:
      table: "sales"
      timestamp_col: "updated_at"
      ...
    checks:
      - name: range_check
        params:
          col: "discount"
          min_val: 0
          max_val: 1
```

No code changes, no reinstall. The pipeline picks it up on the next run.

---

## Built-in checks reference

| Name               | Params                                          | Raises when...                              |
|--------------------|-------------------------------------------------|---------------------------------------------|
| `null_check`       | none                                            | never (informational only)                  |
| `range_check`      | `col`, `min_val`, `max_val`                     | column not found                            |
| `schema_check`     | `expected_cols` (list)                          | never (informational only)                  |
| `duplicate_check`  | `subset` (optional list)                        | never (informational only)                  |

> Note: the built-ins return results but most don't raise. If you want
> validation failures to be surfaced as errors, write a custom check
> that raises on bad data.

---

## Seeing registered checks

```bash
vp checks
```

This lists all built-ins. Custom checks registered in code will also appear
here after registration.

---

## Tips

- Keep check functions pure — read `context["df"]`, return a dict, raise on
  failure. Don't write to disk or mutate shared state inside a check.
- Checks run per-report, not per-row. The DataFrame passed in is the full
  slice of new rows for that report (watermark-filtered).
- If `parallel: true` is set on the report, checks run in threads.
  Make sure your check function is thread-safe (no shared mutable state).
