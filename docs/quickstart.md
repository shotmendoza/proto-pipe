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

All three are config-only — no code changes, no reinstall.

**New source** (`config/sources_config.yaml`):
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

Checks are functions that receive a DataFrame and return a dict. Use the
`@custom_check` decorator and point the pipeline at your module in `pipeline.yaml`.

```python
# my_checks.py

from validation_pipeline import custom_check

@custom_check("no_negatives")
def check_no_negatives(context, col="price"):
    df = context["df"]
    # Return a boolean mask — True where the row fails.
    # Each failing row gets its own entry in validation_flags.
    return {"mask": df[col] < 0}

@custom_check("margin_check")
def check_margin(context, col="margin", threshold=0.2):
    df = context["df"]
    below = df[df[col] < threshold]
    if not below.empty:
        # Raising marks the check as failed and writes a summary flag.
        raise ValueError(f"{len(below)} rows below margin threshold {threshold}")
    return {"violations": 0}
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
      col: "margin"
      threshold: 0.15
```

**Return shapes and what they produce:**

| Return | Flags written |
|---|---|
| `{"mask": pd.Series}` | One flag per row where mask is `True` |
| `{"mask": pd.Series, "flag_when": False}` | One flag per row where mask is `False` |
| Raises an exception | One summary flag with the error message |
| Any other dict | One summary flag with an aggregate description |

See `docs/adding_checks.md` for the full reference.

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

All DuckDB SQL is valid here. Date logic lives in the SQL — no config tokens needed.

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

When `sql_file` is set, the filter system is bypassed entirely — the SQL
runs as-is against the DuckDB connection.

**Shared views** — if multiple SQL files need the same transformation,
define it once in `views_config.yaml` and reference it by name:

```yaml
# config/views_config.yaml
views:
  - name: "clean_sales"
    sql_file: "config/sql/views/clean_sales.sql"
```

```sql
-- config/sql/carrier_a_sales.sql
SELECT order_id, price, region
FROM clean_sales          -- transformation already applied
WHERE region = 'EMEA'
```

After editing a view SQL file, refresh it:

```bash
vp refresh-views
```

See `docs/adding_deliverables.md` for useful DuckDB date expressions and the
full key reference.

---


**Setup & config**

| Task | Command |
|---|---|
| Check path settings | `vp config show` |
| Update a path | `vp config set <key> <value>` |
| List available checks | `vp checks` |

**Ingest**

| Task | Command |
|---|---|
| Load source files | `vp ingest` |
| Load a specific file | `vp update-table <table> <file>` |
| Check failed ingests | `vp ingest-log --status failed` |
| See ingest conflicts | `vp flagged-summary` |
| View conflict rows | `vp flagged-list --table <n>` |
| Export conflicts for correction | `vp export-flagged --table <n>` |
| Apply corrections | `vp import-corrections <file> --table <n>` |
| Clear conflicts without correcting | `vp flagged-clear --table <n>` |
| Scan for conflicts manually | `vp check-null-overwrites --table <n>` |

**Validation**

| Task | Command |
|---|---|
| Run checks | `vp validate` |
| Export validation flags | `vp export-validation` |
| Export flags for one report | `vp export-validation --report <n>` |
| Apply corrections | `vp import-corrections <file> --table <n>` |

**Deliverables**

| Task | Command |
|---|---|
| Produce a deliverable | `vp pull-report <n>` |
| Refresh views manually | `vp refresh-views` |
| Run everything | `vp run-all --deliverable <n>` |
| Run everything, skip conflict block | `vp run-all --deliverable <n> --ignore-flagged` |
