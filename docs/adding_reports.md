# Adding a New Report

Reports define what data to validate and which checks to run against it.
The easiest way is the interactive wizard:

```bash
vp new report
```

The wizard walks you through selecting a table, naming the report, picking
checks, and configuring their column params — then writes the entry to
`config/reports_config.yaml` automatically.

---

## What the wizard does

1. **Select table** — picks from tables that exist in the pipeline DB
2. **Name the report** — defaults to `<table>_validation`
3. **Select checks** — checkbox list of registered checks with docstrings inline.
   Column params show available table columns alongside. Press ESC to go back.
4. **Configure params** — for each selected check, column params offer a
   selector (with alias_map suggestions surfaced first), scalar params are
   free text. Checks with `pd.Series` or `str` column params support
   multi-select to run the check once per column.

After completing the wizard, run `vp validate` to run the new report immediately.

---

## Manual config (no wizard)

Add an entry to `config/reports_config.yaml` directly:

```yaml
reports:
  - name: "sales_validation"
    source:
      type: duckdb
      path: "data/pipeline.db"
      table: "sales"
    options:
      parallel: false
    alias_map:
      - param: col
        column: premium
      - param: col
        column: renewal
    checks:
      - name: null_check
        params: {}
      - name: range_check
        params:
          min_val: 0
          max_val: 1000000
```

---

## alias_map — running one check across multiple columns

`alias_map` maps a check's column param to one or more table columns. At
runtime the check is expanded — one registered instance per column. Scalar
params (like `min_val`) broadcast across all instances.

```yaml
alias_map:
  - param: col
    column: premium
  - param: col
    column: renewal
  - param: col
    column: commission
```

With `range_check` above, this runs three checks: one per column, all
sharing the same `min_val` and `max_val`.

---

## Full key reference

```yaml
reports:
  - name: "my_report"           # unique name — used as watermark key
    source:
      type: duckdb              # only supported type
      path: "data/pipeline.db"  # pipeline DB path
      table: "my_table"         # table to run checks against
    options:
      parallel: false           # true = run checks in parallel threads
    alias_map:                  # column param → table column mappings
      - param: col
        column: premium
    checks:
      - name: null_check        # built-in or custom check name
        params: {}              # scalar params only (column params via alias_map)
      - name: range_check
        params:
          min_val: 0
          max_val: 500
```

---

## After adding a report

```bash
vp validate
```

The new report is picked up automatically. On first run the watermark is
empty, so all existing rows are checked. Subsequent runs only check rows
newer than the last successful run.

To check one table only:

```bash
vp validate --table my_table
```

To view check failures:

```bash
vp errors report my_report
vp errors report export my_report
```

---

## Notes

- `name` must be unique across all reports. Renaming a report resets its
  watermark and triggers a full re-check on the next run.
- If the table doesn't exist yet, run `vp ingest` first.
- Custom checks must be registered before the wizard runs. Set
  `custom_checks_module` in `pipeline.yaml` and run `vp funcs` to verify.
