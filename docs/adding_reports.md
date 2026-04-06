# Adding a New Report

Reports define what data to validate and which checks to run against it.
They live entirely in `config/reports_config.yaml` — no code changes required.

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
    checks:
      - template: standard_null_check       # Reference a template defined in `templates:`
      - template: price_range_check         # Templates have params baked in
      - name: range_check                   # Or define an inline check directly
        params:
          col: "quantity"
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

## Built-in checks and their params

| Check name       | Required params                          | Optional params     |
|------------------|------------------------------------------|---------------------|
| `null_check`     | none                                     | —                   |
| `range_check`    | `col`, `min_val`, `max_val`              | —                   |
| `schema_check`   | `expected_cols` (list of column names)   | —                   |
| `duplicate_check`| none                                     | `subset` (list)     |

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

To check only your new report's table:

```bash
vp validate --table my_table
```

---

## Report-to-report dependencies

A report can read from another report's output table. The pipeline detects
this automatically and ensures the upstream report runs first — no extra
config is needed.

**How it works:** every report writes its results to a table named
`<source_table>_report` by default (or an explicit `target_table` if
configured). If report B's `source.table` matches that name, B is treated
as depending on A and will always run after A completes.

**Example — enriched sales built on top of validated sales:**

```yaml
reports:
  - name: "sales_validation"
    source:
      table: "sales"          # reads from the raw sales table
      timestamp_col: "updated_at"
    checks:
      - template: standard_null_check

  - name: "enriched_sales"
    source:
      table: "sales_report"   # reads from sales_validation's output table
      timestamp_col: "updated_at"
    checks:
      - template: price_range_check
```

`vp validate` will always run `sales_validation` before `enriched_sales`.
If `sales_validation` errors, `enriched_sales` is skipped automatically
with a message naming the upstream failure.

**Cycle detection:** if two reports depend on each other (directly or
indirectly), `vp validate` will exit with an error naming the reports
involved before running anything.

---

## Notes

- `name` must be unique across all reports. It is used as the watermark key,
  so renaming a report resets its watermark and triggers a full re-check.
- `timestamp_col` must exist in the table before the first ingest. If the
  source file doesn't include it, add it to `sources_config.yaml` and make
  sure incoming files carry the column.
- If the table doesn't exist yet, run `vp ingest` first to create it.
