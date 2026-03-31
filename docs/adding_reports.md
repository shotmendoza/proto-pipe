# Adding a New Report

Reports define what data to validate and which checks to run against it.
They live entirely in `config/reports_config.yaml` — no code changes required.

The easiest way to add a report is with the interactive wizard:

```bash
vp new-report
```

The wizard walks you through selecting a table, naming the report, choosing
checks, filling in params, and building an `alias_map` if the same check
needs to run across multiple columns. It writes the config entry for you.

---

## Minimal example

```yaml
reports:
  - name: "my_new_report"
    source:
      type: duckdb
      path: "data/pipeline.db"
      table: "my_table"
      timestamp_col: "updated_at"
    checks:
      - template: standard_null_check
```

That's the smallest valid report. It points at a table, picks a timestamp
column for watermark tracking, and runs one check.

---

## Full key reference

```yaml
reports:
  - name: "my_new_report"           # Unique name — used in deliverables_config.yaml
                                    # and as the watermark key
    source:
      type: duckdb                  # Only supported type currently
      path: "data/pipeline.db"      # Path to the DuckDB file
      table: "my_table"             # Table to query
      timestamp_col: "updated_at"   # Column used for watermark filtering.
                                    # Must exist in the table.
    options:
      parallel: true                # Run this report's checks in parallel threads.
                                    # Default: false. Safe to enable for independent checks.

    alias_map:                      # Optional. Maps check params to table columns.
      - param: col                  # The param name used by the check function
        column: "Coverage A"        # The actual column name in this report's table
      - param: col
        column: "Coverage B"        # Checks using 'col' run once per alias_map entry

    checks:
      - template: standard_null_check       # Reference a template defined in `templates:`
      - template: price_range_check         # Templates have params baked in
      - name: range_check                   # Or define an inline check directly
        params:
          col: "quantity"                   # Scalar params — not affected by alias_map
          min_val: 1
          max_val: 9999
```

---

## Using templates vs inline checks

**Templates** (defined once, reused across reports):

```yaml
templates:
  standard_null_check:
    name: null_check              # Maps to a built-in check function

  price_range_check:
    name: range_check
    params:
      col: "price"
      min_val: 0
      max_val: 500
```

Reference them in any report with `template: <template_name>`.

**Inline checks** (defined directly on the report, not reusable):

```yaml
checks:
  - name: range_check
    params:
      col: "stock_level"
      min_val: 0
      max_val: 10000
```

Use inline checks when the params are specific to one report and not worth
sharing as a template.

---

## Alias map — running a check across multiple columns

When the same check applies to multiple columns in a report (e.g. several
coverage columns), use `alias_map` instead of repeating the check entry.
The check runs once per `alias_map` entry for the matching param. Scalar
params broadcast across all runs.

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
```

This runs `range_check` three times — once for each coverage column —
with `min_val` and `max_val` broadcast to all three.

The `alias_map` is also carrier-specific: two reports using the same check
can map the same param to different column names without any conflict.

Use `vp new-report` to build the alias_map interactively.

---

## Built-in checks and their params

| Check name         | Params                                         | What it checks              |
|--------------------|------------------------------------------------|-----------------------------|
| `null_check`       | none                                           | No nulls in any column      |
| `range_check`      | `col: str`, `min_val: float`, `max_val: float` | Value within range          |
| `schema_check`     | `expected_cols: list[str]`                     | Exact column set matches    |
| `duplicate_check`  | `subset: list[str]` (optional)                 | No duplicate rows           |

Run `vp checks` from the CLI to see the current list at any time.

---

## After adding a report

No reinstall needed. Just run:

```bash
vp validate
```

The new report will be picked up automatically. On first run the watermark
is empty, so all existing rows in the table are checked. Subsequent runs
only check rows newer than the last successful run.

---

## Notes

- `name` must be unique across all reports. It is used as the watermark key,
  so renaming a report resets its watermark and triggers a full re-check.
- `timestamp_col` must exist in the table before the first ingest. If the
  source file doesn't include it, add it to `sources_config.yaml` and make
  sure incoming files carry the column.
- If the table doesn't exist yet, run `vp ingest` first to create it.
- If alias_map params have multiple entries, all multi-entry params must have
  the same number of entries. A single-entry param broadcasts silently.