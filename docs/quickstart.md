# Quickstart

This covers the three things you'll do regularly once the pipeline is set up.
For first-time installation and config, see `docs/first_time_setup.md`.

---

## The three flows

```
1. Run everything        vp run-all --deliverable <name>
2. Fix ingest conflicts  vp flagged-summary → export → fix → import
3. Fix validation flags  vp export-validation → fix → import
```

---

## 1. Daily / monthly run

```bash
vp run-all --deliverable monthly_pack
```

What it does: ingest → validate → refresh views → produce deliverable.

**If it stops with an ingest conflict warning:**
Rows arrived whose values differ from what's already in the table.
These block the deliverable until resolved.

```bash
vp flagged-summary                                           # which tables, how many
vp export-flagged --table sales                              # export to CSV
# edit the CSV — fix the values, leave _flag_id untouched
vp import-corrections flagged_sales_2026-03-26.csv --table sales
vp run-all --deliverable monthly_pack                        # re-run clean
```

To produce the deliverable without resolving conflicts:

```bash
vp run-all --deliverable monthly_pack --ignore-flagged
```

**If it completes with a validation warning:**
Rows in the report table failed a check. The deliverable was still produced.
Review when ready — these don't block anything.

```bash
vp export-validation                    # Detail + Summary sheets
# edit the Detail sheet — fix the values, leave _flag_id untouched
vp import-corrections validation_2026-03-26.xlsx --table sales
vp validate                             # confirm clean
```

---

## 2. Loading a new source file

Drop the file into `incoming_dir` and run:

```bash
vp ingest
```

Files already ingested are skipped automatically. To check why a file failed:

```bash
vp ingest-log --status failed
```

To load a specific file directly (bypassing directory scan):

```bash
vp update-table sales data/incoming/sales_2026-03.csv
```

---

## 3. Adding a new source, report, or deliverable

All three are config-only — no code changes, no reinstall. Use the
interactive wizards to avoid editing YAML directly:

```bash
vp new-source        # define a new data source
vp new-report        # define a new report with checks and alias_map
vp new-deliverable   # define a new output file
```

Or edit configs manually. **New source** (`config/sources_config.yaml`):

```yaml
sources:
  - name: "orders"
    patterns: ["orders_*.csv"]
    target_table: "orders"
    timestamp_col: "updated_at"
    primary_key: "order_id"
```

Then `vp ingest` — the table is created automatically on first load.

**New report** (`config/reports_config.yaml`):

```yaml
reports:
  - name: "orders_validation"
    source:
      type: duckdb
      path: "data/pipeline.db"
      table: "orders"
      timestamp_col: "updated_at"
    checks:
      - name: null_check
      - name: range_check
        params: {col: "total", min_val: 0, max_val: 100000}
```

Then `vp validate` — picks it up automatically.

**New deliverable** (`config/deliverables_config.yaml`):

```yaml
deliverables:
  - name: "orders_pack"
    format: xlsx
    filename_template: "orders_{date}.xlsx"
    reports:
      - name: "orders_validation"
        sheet: "Orders"
        filters:
          date_filters:
            - col: "updated_at"
              to: "end_of_last_month"
```

Then `vp pull-report orders_pack`.

---

## 4. Writing a custom check

Checks return `pd.Series[bool]` — `True` where a row passes, `False` where
it fails. Use the `@custom_check` decorator and point the pipeline at your
module in `pipeline.yaml`.

```python
# my_checks.py

import pandas as pd
from proto_pipe.checks.helpers import custom_check


@custom_check("no_negatives")
def check_no_negatives(context: dict, col: str = "price") -> pd.Series:
    df = context["df"]
    return df[col] >= 0   # True = passes, False = flagged


@custom_check("margin_check")
def check_margin(context: dict, col: str = "margin", threshold: float = 0.2) -> pd.Series:
    df = context["df"]
    return df[col] >= threshold
```

Register the module in `pipeline.yaml`:

```yaml
custom_checks_module: "my_checks.py"
```

Reference the check in `reports_config.yaml` like any built-in:

```yaml
checks:
  - name: no_negatives
    params:
      col: "price"
  - name: margin_check
    params:
      threshold: 0.15
      # col resolved from alias_map if defined on the report
```

**Transforms** — use `kind="transform"` to modify column values rather than
flag rows. Transforms run after all checks and write back to the report table.

```python
@custom_check("normalize_transaction_type", kind="transform")
def transform_tx_type(context: dict, col: str = "transaction_type") -> pd.Series:
    result = context["df"][col].replace({"Issuance": "Reinstatement"})
    result.name = col
    return result
```

See `docs/adding_checks.md` for the full reference including alias_map and
SQL macros.

---

## 5. Adding a SQL file for a deliverable

Use a SQL file when a deliverable needs joins across tables, carrier-specific
columns, or date logic that goes beyond simple filters.

**Step 1 — Write the SQL** (`config/sql/carrier_a_sales.sql`):

```sql
SELECT
    s.order_id,
    s.price,
    s.quantity,
    c.customer_name,
    c.region
FROM sales s
JOIN customers c ON s.customer_id = c.customer_id
WHERE s.order_date <= (date_trunc('month', current_date) - INTERVAL '1 day')
  AND c.region = 'EMEA'
```

**Step 2 — Reference the file in `deliverables_config.yaml`:**

```yaml
deliverables:
  - name: "carrier_a_pack"
    format: xlsx
    filename_template: "carrier_a_{date}.xlsx"
    reports:
      - name: "carrier_a_sales"
        sheet: "Sales"
        sql_file: "config/sql/carrier_a_sales.sql"
```

**Shared views** — define table-level transformations once in
`views_config.yaml` and reference them by name in any SQL file.

**SQL macros** — define reusable column-level expressions with `vp new-macro`
and call them in any SQL file:

```bash
vp new-macro normalize_transaction_type
# edit macros/normalize_transaction_type.sql
```

```sql
SELECT normalize_transaction_type(transaction_type) AS transaction_type
FROM sales
```

After adding or editing a macro, run `vp db-init` to re-register it.

See `docs/adding_deliverables.md` for useful DuckDB date expressions and the
full key reference.

---

## Quick reference

**Setup & config**

| Task                       | Command                       |
|----------------------------|-------------------------------|
| Check path settings        | `vp config show`              |
| Update a path              | `vp config set <key> <value>` |
| List available checks      | `vp checks`                   |
| Scaffold a new source      | `vp new-source`               |
| Scaffold a new report      | `vp new-report`               |
| Scaffold a new deliverable | `vp new-deliverable`          |
| Scaffold a SQL query file  | `vp new-sql <n>`              |
| Scaffold a macro           | `vp new-macro <n>`            |
| Reset a report table       | `vp table-reset --report <n>` |

**Ingest**

| Task                               | Command                                    |
|------------------------------------|--------------------------------------------|
| Load source files                  | `vp ingest`                                |
| Load a specific file               | `vp update-table <table> <file>`           |
| Check failed ingests               | `vp ingest-log --status failed`            |
| See ingest conflicts               | `vp flagged-summary`                       |
| View conflict rows                 | `vp flagged-list --table <n>`              |
| Export conflicts for correction    | `vp export-flagged --table <n>`            |
| Apply corrections                  | `vp import-corrections <file> --table <n>` |
| Clear conflicts without correcting | `vp flagged-clear --table <n>`             |

**Validation**

| Task                        | Command                             |
|-----------------------------|-------------------------------------|
| Run checks                  | `vp validate`                       |
| Export validation flags     | `vp export-validation`              |
| Export flags for one report | `vp export-validation --report <n>` |

**Deliverables**

| Task                                | Command                                         |
|-------------------------------------|-------------------------------------------------|
| Produce a deliverable               | `vp pull-report <n>`                            |
| Refresh views manually              | `vp refresh-views`                              |
| Re-register macros                  | `vp db-init`                                    |
| Run everything                      | `vp run-all --deliverable <n>`                  |
| Run everything, skip conflict block | `vp run-all --deliverable <n> --ignore-flagged` |