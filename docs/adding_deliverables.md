# Adding a New Deliverable

Deliverables define what gets written to disk — which reports to include,
what format to use, how to query the data, and where to put the output.
They live entirely in `config/deliverables_config.yaml`.

Each report inside a deliverable has two query paths:

| Path | When to use |
|---|---|
| `sql_file` | Joins across tables, carrier-specific columns, custom date logic |
| `filters` | Simple single-table queries with date and field filters |

---

## Minimal examples

**With a SQL file (joins, custom logic):**

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

**With filters (simple single-table query):**

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
  - name: "my_export"                         # Unique name — used on the CLI:
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
        sql_file: "config/sql/my_report.sql"  # Path to a .sql file.
                                              # Executes directly — filters ignored.
                                              # Date logic lives inside the SQL.

        # --- Query path B: filters (used when sql_file is absent) ---
        filters:
          date_filters:
            - col: "order_date"
              from: "start_of_month"          # Optional. Supports dynamic tokens.
              to:   "end_of_last_month"        # Optional. Supports dynamic tokens.
          field_filters:
            - col: "region"
              values: ["EMEA", "APAC"]
```

Only one of `sql_file` or `filters` should be set per report. If `sql_file`
is present it takes precedence and `filters` is ignored entirely.

---

## SQL files

SQL files contain a single `SELECT` statement executed directly against
the DuckDB connection. They can join any tables that exist in `pipeline.db`,
use any DuckDB SQL features, and handle date logic natively.

```sql
-- config/sql/carrier_a_sales.sql
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

SQL files are fully self-contained — open the file and you see exactly
what data the deliverable will produce.

---

## Dynamic date tokens (filter path only)

When using `filters:` instead of `sql_file:`, date values support
dynamic tokens that resolve at runtime. Hardcoded dates like `"2026-01-01"`
still work and pass through unchanged.

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

Omitting `from:` or `to:` means no lower or upper bound — all rows on
that end are included. Omitting both returns all rows.

```yaml
date_filters:
  - col: "order_date"
    to: "end_of_last_month"     # inception to end of last month
  - col: "updated_at"
    to: "end_of_last_month"     # same token, second date column
```

---

## Format: `xlsx` vs `csv`

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

Produces: `monthly_pack_2026-03-26.xlsx` with two sheets.

**csv** — one file per report:

```yaml
format: csv
filename_template: "{report_name}_{date}.csv"
reports:
  - name: "sales_report"
    sql_file: "config/sql/sales.sql"
```

Produces: `sales_report_2026-03-26.csv`.

---

## CLI date overrides

CLI date flags only apply to filter-based reports. They are ignored for
any report that has a `sql_file` — date logic for those lives in the SQL.

```bash
# Override date range for filter-based reports
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

## Carrier-specific pattern

Each carrier gets its own deliverable pointing to its own SQL file.
Shared reports (e.g. inventory) can still use filters and be included
in multiple deliverables without duplication.

```yaml
deliverables:
  - name: "carrier_a_pack"
    format: xlsx
    filename_template: "carrier_a_{date}.xlsx"
    reports:
      - name: "carrier_a_sales"
        sheet: "Sales"
        sql_file: "config/sql/carrier_a_sales.sql"   # carrier-specific join + columns
      - name: "inventory_validation"
        sheet: "Inventory"
        filters:                                      # shared report, filter path
          date_filters:
            - col: "updated_at"
              to: "end_of_last_month"

  - name: "carrier_b_pack"
    format: xlsx
    filename_template: "carrier_b_{date}.xlsx"
    reports:
      - name: "carrier_b_sales"
        sheet: "Sales"
        sql_file: "config/sql/carrier_b_sales.sql"   # different SQL, different shape
      - name: "inventory_validation"
        sheet: "Inventory"
        filters:
          date_filters:
            - col: "updated_at"
              to: "end_of_last_month"
```

---

## Tracking what was produced

Every run is logged to the `report_runs` table in `pipeline.db`:

```sql
SELECT * FROM report_runs ORDER BY created_at DESC;
```

Columns: `deliverable_name`, `report_name`, `filename`, `output_dir`,
`filters_applied`, `row_count`, `format`, `created_at`.

---

## Notes

- `name` must be unique across all deliverables.
- Each report `name` must match a name in `reports_config.yaml` when
  using the filter path. For `sql_file` reports this lookup is skipped —
  the name is only used for sheet naming and logging.
- Output files are never auto-deleted. Re-running on the same date
  overwrites the file.
- `output_dir` on the deliverable overrides `pipeline.yaml`. If neither
  is set, defaults to `output/reports/`.
