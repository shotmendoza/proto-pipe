# Adding a New Report

Reports are defined in `reports_config.yaml`. Each report pulls data from a
DuckDB table, runs a list of checks against it, and advances a watermark so
only new or modified rows are checked on the next run.

---

## Quickstart

Add an entry under `reports:` in your `reports_config.yaml`:

```yaml
reports:
  - name: "my_new_report"
    source:
      type: duckdb
      path: "${PIPELINE_DB_PATH}"
      table: "my_table"
      timestamp_col: "updated_at"
    options:
      parallel: true
    checks:
      - template: standard_null_check
      - name: range_check
        params:
          col: "amount"
          min_val: 0
          max_val: 10000
```

No code changes required. The next pipeline run will pick it up automatically.

---

## Report keys

### `name` (required)
Unique identifier for the report. Used in watermark tracking and result output.
Must be unique across all reports.

```yaml
name: "daily_sales_validation"
```

---

### `source` (required)
Defines where the report pulls its data from.

| Key             | Required | Description                                                  |
|-----------------|----------|--------------------------------------------------------------|
| `type`          | yes      | Always `duckdb` for now                                      |
| `path`          | yes      | Path to the DuckDB file. Use `${ENV_VAR}` for env variables  |
| `table`         | yes      | Table name inside the DuckDB file to query                   |
| `timestamp_col` | yes      | Column used for watermark filtering (must be a timestamp)    |

```yaml
source:
  type: duckdb
  path: "${PIPELINE_DB_PATH}"
  table: "sales"
  timestamp_col: "updated_at"
```

> **Note:** `timestamp_col` must exist in the table. On the first run all rows
> are loaded. On subsequent runs only rows where `timestamp_col > last_run` are
> loaded. If no new rows are found the report is skipped entirely.

---

### `options` (optional)
Controls how the report runs.

| Key        | Type    | Default | Description                                              |
|------------|---------|---------|----------------------------------------------------------|
| `parallel` | boolean | `false` | Run checks within this report in parallel using threads  |

```yaml
options:
  parallel: true
```

> Parallelising checks within a report is most useful when checks are IO-bound
> (e.g. hitting an external API). For CPU-bound or fast in-memory checks,
> sequential is usually fine.

---

### `checks` (required)
A list of checks to run against the report's data. Each entry is either a
**template reference** or an **inline check**.

#### Template reference
Reuses a check defined under `templates:`. No params needed here — they were
baked in when the template was defined.

```yaml
checks:
  - template: standard_null_check
  - template: price_range_check
```

#### Inline check
Defines a one-off check directly on the report. Use this for checks that are
specific to this report and unlikely to be reused.

```yaml
checks:
  - name: range_check
    params:
      col: "discount_pct"
      min_val: 0
      max_val: 100
```

| Key      | Required | Description                                          |
|----------|----------|------------------------------------------------------|
| `name`   | yes      | Name of a built-in or registered custom check        |
| `params` | varies   | Parameters for the check (see check reference below) |

You can mix templates and inline checks freely in the same report:

```yaml
checks:
  - template: standard_null_check       # reusable template
  - template: price_range_check         # reusable template
  - name: range_check                   # one-off inline check
    params:
      col: "tax_rate"
      min_val: 0
      max_val: 1
```

---

## Reusing checks across reports with templates

If you find yourself writing the same inline check on multiple reports, move
it to a template:

```yaml
templates:
  tax_rate_check:
    name: range_check
    params:
      col: "tax_rate"
      min_val: 0
      max_val: 1

reports:
  - name: "sales_report"
    ...
    checks:
      - template: tax_rate_check   # reference it here

  - name: "invoices_report"
    ...
    checks:
      - template: tax_rate_check   # and here — no duplication
```

### Template keys

| Key      | Required | Description                                       |
|----------|----------|---------------------------------------------------|
| `name`   | yes      | The built-in or custom check function name        |
| `params` | varies   | Parameters baked into the check at load time      |

---

## Built-in check reference

### `null_check`
Checks all columns for null values. No params required.

**Returns:**
```json
{ "null_counts": { "col_a": 0, "col_b": 3 }, "has_nulls": true }
```

---

### `range_check`
Checks that a column's values fall within `[min_val, max_val]`.

| Param     | Type  | Required | Description              |
|-----------|-------|----------|--------------------------|
| `col`     | str   | yes      | Column name to check     |
| `min_val` | float | yes      | Inclusive lower bound    |
| `max_val` | float | yes      | Inclusive upper bound    |

**Returns:**
```json
{ "col": "price", "min_val": 0, "max_val": 500, "violations": 3, "violation_indices": [4, 17, 92] }
```

---

### `schema_check`
Checks the table has exactly the expected columns.

| Param           | Type       | Required | Description                    |
|-----------------|------------|----------|--------------------------------|
| `expected_cols` | list[str]  | yes      | List of column names expected  |

**Returns:**
```json
{ "missing_cols": ["customer_id"], "extra_cols": ["legacy_id"], "matches": false }
```

---

### `duplicate_check`
Checks for duplicate rows, optionally scoped to a subset of columns.

| Param    | Type       | Required | Description                                      |
|----------|------------|----------|--------------------------------------------------|
| `subset` | list[str]  | no       | Columns to check for duplicates. Default: all.   |

**Returns:**
```json
{ "duplicate_count": 2, "has_duplicates": true, "subset": ["order_id"] }
```
