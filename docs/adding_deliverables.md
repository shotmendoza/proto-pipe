# Adding a New Deliverable

Deliverables define what gets written to disk — which reports to include,
what format to use, how to query the data, and where to put the output.

The easiest way is the interactive wizard:

```bash
vp new deliverable
```

The wizard walks you through selecting reports, naming the deliverable,
choosing format, and scaffolding a SQL file — then writes to
`config/deliverables_config.yaml` automatically.

---

## Two query paths per report

| Path | When to use |
|---|---|
| `sql_file` | Joins across tables, carrier-specific column names, custom date logic |
| `filters` | Simple single-table queries with date and field filters |

Only one should be set per report. If `sql_file` is present it takes
precedence and `filters` is ignored.

---

## Minimal examples

**With a SQL file:**

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

**With filters:**

```yaml
deliverables:
  - name: "ops_daily_drop"
    format: csv
    filename_template: "{report_name}_{date}.csv"
    reports:
      - name: "daily_sales_validation"
        filters:
          date_filters:
            - col: "order_date"
              to: "end_of_last_month"
```

---

## Full key reference

```yaml
deliverables:
  - name: "my_export"                         # Unique name — used on CLI:
                                              #   vp pull-report my_export
    format: xlsx                              # "xlsx" = one file, one sheet per report
                                              # "csv"  = one file per report
    filename_template: "export_{date}.xlsx"   # {date} = YYYY-MM-DD of run
                                              # {report_name} = report name (csv only)
    output_dir: "output/custom/"              # Optional. Overrides pipeline.yaml output_dir.

    reports:
      - name: "my_report"                     # Must match a name in reports_config.yaml
        sheet: "My Sheet"                     # xlsx only. Defaults to report name if omitted.

        # --- Query path A: SQL file ---
        sql_file: "config/sql/my_report.sql"  # Executes directly — filters ignored.

        # --- Query path B: filters ---
        filters:
          date_filters:
            - col: "order_date"
              from: "start_of_month"
              to:   "end_of_last_month"
          field_filters:
            - col: "region"
              values: ["EMEA", "APAC"]
```

---

## SQL files

SQL files contain a single `SELECT` executed directly against `pipeline.db`.
They can join any tables, use any DuckDB SQL features, and handle date logic
natively.

```sql
-- config/sql/carrier_a_sales.sql
SELECT
    s.order_id,
    s.gross_premium,       -- Carrier A's column name for premium
    s.quantity,
    c.customer_name
FROM sales s
JOIN customers c ON s.customer_id = c.customer_id
WHERE s.order_date <= (date_trunc('month', current_date) - INTERVAL '1 day')
  AND c.region = 'EMEA'
```

**Useful DuckDB date expressions:**

| Goal | Expression |
|---|---|
| End of last month | `date_trunc('month', current_date) - INTERVAL '1 day'` |
| Start of this month | `date_trunc('month', current_date)` |
| Start of last month | `date_trunc('month', current_date) - INTERVAL '1 month'` |
| Today | `current_date` |
| 30 days ago | `current_date - INTERVAL '30 days'` |
| Start of this quarter | `date_trunc('quarter', current_date)` |
| End of last quarter | `date_trunc('quarter', current_date) - INTERVAL '1 day'` |

---

## Carrier-specific pattern and alias_map

Different carriers often use different column names for the same data.
The pipeline handles this at two levels:

**In the SQL file** — write the SQL using the carrier's actual column names:

```sql
-- carrier_a_sales.sql — Carrier A calls it gross_premium
SELECT order_id, gross_premium, region FROM sales WHERE region = 'EMEA'

-- carrier_b_sales.sql — Carrier B calls it written_premium
SELECT order_id, written_premium, region FROM sales WHERE region = 'APAC'
```

**In reports_config.yaml via alias_map** — map different column names to
the same check parameter so one check function runs against both:

```yaml
reports:
  - name: "carrier_a_validation"
    source:
      table: "carrier_a_sales"
    alias_map:
      - param: col
        column: gross_premium      # Carrier A's column name
    checks:
      - name: null_check

  - name: "carrier_b_validation"
    source:
      table: "carrier_b_sales"
    alias_map:
      - param: col
        column: written_premium    # Carrier B's column name
    checks:
      - name: null_check           # same check function, different column
```

The check function is written once with a generic `col` parameter.
Each carrier's report maps their specific column to it via `alias_map`.
The `vp new report` wizard handles this interactively — column params
show available columns from the selected table with alias_map suggestions
surfaced first.

---

## Shared views

If multiple deliverables need the same transformation — standardising region
codes, zeroing out negatives, joining a lookup table — define it once as a
view and reference it in any SQL file.

```yaml
# config/views_config.yaml
views:
  - name: "clean_sales"
    sql_file: "config/sql/views/clean_sales.sql"
```

```sql
-- config/sql/views/clean_sales.sql
SELECT
    order_id,
    CASE WHEN region = 'Europe' THEN 'EMEA' ELSE region END AS region,
    GREATEST(price, 0) AS price,
    order_date
FROM sales
```

```sql
-- config/sql/carrier_a_sales.sql — reference the view like a table
SELECT order_id, price, region
FROM clean_sales
WHERE region = 'EMEA'
```

**Creation order matters** — if a view references another view, list the
dependency first in `views_config.yaml`.

Views are refreshed by `vp refresh-views` and automatically before
deliverables in `vp run-all`.

---

## Dynamic date tokens (filter path only)

| Token | Resolves to |
|---|---|
| `today` | Current date |
| `start_of_month` | First day of current month |
| `end_of_month` | Last day of current month |
| `start_of_last_month` | First day of previous month |
| `end_of_last_month` | Last day of previous month |
| `start_of_quarter` | First day of current quarter |
| `end_of_quarter` | Last day of current quarter |
| `start_of_last_quarter` | First day of previous quarter |
| `end_of_last_quarter` | Last day of previous quarter |
| `today-Nd` | N days ago (e.g. `today-30d`) |
| `today+Nd` | N days from now (e.g. `today+7d`) |

Omitting `from:` or `to:` means no bound on that end.

---

## Format: xlsx vs csv

**xlsx** — all reports in one file, one sheet each:

```yaml
format: xlsx
filename_template: "monthly_pack_{date}.xlsx"
reports:
  - name: "sales_report"
    sheet: "Sales"
    sql_file: "config/sql/sales.sql"
  - name: "inventory_report"
    sheet: "Inventory"
    filters:
      date_filters:
        - col: "updated_at"
          from: "start_of_month"
```

**csv** — one file per report:

```yaml
format: csv
filename_template: "{report_name}_{date}.csv"
reports:
  - name: "sales_report"
    sql_file: "config/sql/sales.sql"
```

---

## CLI date overrides

Date flags only apply to filter-based reports. SQL file reports ignore them.

```bash
vp pull-report my_export \
  --date-col order_date \
  --date-from 2026-01-01 \
  --date-to   2026-03-31
```

---

## Running a deliverable

```bash
vp pull-report my_export
vp run-all --deliverable my_export
```

---

## Tracking what was produced

```sql
SELECT * FROM report_runs ORDER BY created_at DESC;
```

Columns: `deliverable_name`, `report_name`, `filename`, `output_dir`,
`filters_applied`, `row_count`, `format`, `created_at`.

---

## Notes

- `name` must be unique across all deliverables.
- Output files are never auto-deleted. Re-running on the same date overwrites.
- `output_dir` on the deliverable overrides `pipeline.yaml`. If neither is
  set, defaults to `output/reports/`.