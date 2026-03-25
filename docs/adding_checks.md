# Adding a New Check

Checks are functions that receive a `context` dict (containing a pandas
DataFrame) and return a dict of results. Once registered, a check is available
by name in `reports_config.yaml` with no further wiring required.

---

## Quickstart

```python
from validation_pipeline import check_registry
from validation_pipeline.checks.registry_helpers import register_custom_check

def check_no_future_dates(context, col="order_date"):
    import pandas as pd
    df = context["df"]
    future = df[df[col] > pd.Timestamp.now()]
    return {"future_date_count": len(future), "col": col}

register_custom_check(
    name="future_date_check",
    func=check_no_future_dates,
    check_registry=check_registry,
)
```

Then reference it in your config immediately:

```yaml
checks:
  - name: future_date_check
    params:
      col: "order_date"
```

---

## Check function signature

Every check must follow this signature:

```python
def my_check(context: dict, **params) -> dict:
    df = context["df"]   # always a pandas DataFrame
    ...
    return { ... }       # always a dict
```

| Argument  | Type   | Description                                                        |
|-----------|--------|--------------------------------------------------------------------|
| `context` | dict   | Always the first argument. Contains `df` (the DataFrame to check) |
| `**params`| any    | Your check's own parameters, baked in at registration time         |

The return value can contain any keys you like. Aim to include a clear
indicator of whether the check found issues (e.g. a `violations` count or a
`passed` boolean) so results are easy to interpret in reports.

---

## Registering a check

Use `register_custom_check` from `registry_helpers`. This registers the check
in both the `CheckRegistry` (so the runner can call it) and `BUILT_IN_CHECKS`
(so the config loader can find it by name in YAML).

```python
from validation_pipeline import check_registry
from validation_pipeline.checks.registry_helpers import register_custom_check

register_custom_check(
    name="my_check",       # name used in reports_config.yaml
    func=my_check_func,    # the function defined above
    check_registry=check_registry,
)
```

### `register_custom_check` parameters

| Parameter        | Required | Description                                                              |
|------------------|----------|--------------------------------------------------------------------------|
| `name`           | yes      | The name used to reference this check in YAML config                     |
| `func`           | yes      | The check function                                                       |
| `check_registry` | yes      | The `CheckRegistry` instance (import `check_registry` from the package)  |
| `**default_params` | no     | Optional params baked in via `functools.partial` at registration time    |

### With default params baked in

If you always use the same params for a check, you can bake them in at
registration so you don't need to specify them in the config:

```python
register_custom_check(
    name="order_date_check",
    func=check_no_future_dates,
    check_registry=check_registry,
    col="order_date",       # baked in — no params needed in YAML
)
```

```yaml
checks:
  - name: order_date_check   # no params block needed
```

### Without default params

If you want the config to supply the params each time:

```python
register_custom_check(
    name="future_date_check",
    func=check_no_future_dates,
    check_registry=check_registry,
    # no default params — caller must supply col in YAML
)
```

```yaml
checks:
  - name: future_date_check
    params:
      col: "shipped_date"
```

---

## Where to put your check registration

Call `register_custom_check` **before** `register_from_config` runs, so the
config loader can find your check by name. The recommended place is in your
project's entrypoint, before the pipeline runs:

```python
# main.py
from validation_pipeline import check_registry, report_registry
from validation_pipeline.config.loader import load_config, register_from_config
from validation_pipeline.checks.registry_helpers import register_custom_check
from validation_pipeline.runner import run_all_reports
from validation_pipeline.watermark import WatermarkStore
import os

# 1. Register custom checks first
register_custom_check("future_date_check", check_no_future_dates, check_registry)
register_custom_check("margin_check", check_margin, check_registry, threshold=0.2)

# 2. Then load config — custom checks are now available by name
config = load_config("config/reports_config.yaml")
register_from_config(config, check_registry, report_registry)

# 3. Run
watermark_store = WatermarkStore(os.environ["WATERMARK_DB_PATH"])
results = run_all_reports(report_registry, check_registry, watermark_store)
```

---

## Full example

```python
# checks/my_checks.py — keep your custom checks in their own module

def check_margin(context: dict, col: str = "margin", threshold: float = 0.2) -> dict:
    """Check that margin values are above a minimum threshold."""
    df = context["df"]
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found")
    below = df[df[col] < threshold]
    return {
        "col": col,
        "threshold": threshold,
        "violations": len(below),
        "violation_indices": below.index.tolist(),
    }


def check_no_future_dates(context: dict, col: str = "order_date") -> dict:
    """Check that a date column contains no future dates."""
    import pandas as pd
    df = context["df"]
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found")
    future = df[df[col] > pd.Timestamp.now()]
    return {
        "col": col,
        "future_date_count": len(future),
        "has_future_dates": len(future) > 0,
    }
```

```python
# main.py — register them before loading config

from checks.my_checks import check_margin, check_no_future_dates
from validation_pipeline import check_registry
from validation_pipeline.checks.registry_helpers import register_custom_check

register_custom_check("margin_check", check_margin, check_registry)
register_custom_check("future_date_check", check_no_future_dates, check_registry)
```

```yaml
# reports_config.yaml — use them like any built-in

templates:
  low_margin_check:
    name: margin_check
    params:
      col: "margin"
      threshold: 0.15

reports:
  - name: "sales_validation"
    source:
      type: duckdb
      path: "${PIPELINE_DB_PATH}"
      table: "sales"
      timestamp_col: "updated_at"
    checks:
      - template: low_margin_check
      - name: future_date_check
        params:
          col: "order_date"
```

---

## Raising errors vs returning violations

A check should **raise** an exception only for unexpected failures (missing
column, wrong type). For expected validation failures, **return them in the
result dict** — the runner catches exceptions and marks the check as `failed`,
whereas a result with `violations > 0` is still a `passed` check status that
your reporting layer can act on separately.

```python
def check_range(context, col, min_val, max_val):
    df = context["df"]
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found")   # unexpected — raise
    violations = df[~df[col].between(min_val, max_val)]
    return {"violations": len(violations)}               # expected — return
```
