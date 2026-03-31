# Adding a New Deliverable

Deliverables define what gets written to disk — which reports to include,
what format to use, how to query the data, and where to put the output.
They live entirely in `config/deliverables_config.yaml`.

Each report inside a deliverable has two query paths:

| Path       | When to use                                                      |
|------------|------------------------------------------------------------------|
| `sql_file` | Joins across tables, carrier-specific columns, custom date logic |
| `filters`  | Simple single-table queries with date and field filters          |

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

## Scaffolding a SQL file

Use `vp new-sql` to scaffold an annotated SQL file for any combination of
reports without creating a new deliverable entry. This is the fastest way
to start writing a carrier-specific query.

```bash
vp new-sql carrier_a_sales
```

The wizard asks which reports to include, then generates a `.sql` file with:

- Notes about which transforms are already applied to each table
- A list of available macros you can call inline
- A `LEFT JOIN` stub pre-filled with primary keys where they can be resolved
- Inline comments on how to add columns from joined tables
- A `WHERE _ingested_at >= '<from_date>'` filter to edit as needed

Example output for a two-table query with a macro registered:

```sql
-- carrier_a_sales.sql
-- Deliverable query for: carrier_a_sales
--
-- The tables below have transforms applied before this query runs.
-- Any @custom_check(kind='transform') functions registered for
-- these reports are already reflected in the data.
--
-- Available macros (call inline in your SELECT):
--   normalize_transaction_type(val)
--
-- Columns prefixed with _ are internal pipeline columns
-- (e.g. _ingested_at) — exclude from your SELECT.

SELECT
    a.*
    -- Example macro usage (uncomment and adapt):
    -- , normalize_transaction_type(a.<col>) AS <col>
    -- , b.<column>  -- add columns from inventory as needed
FROM sales a
LEFT JOIN inventory b
    ON a.order_id = b.order_id
WHERE a._ingested_at >= '<from_date>'
ORDER BY a._ingested_at DESC;
```

Once you've edited the file, wire it up in `deliverables_config.yaml`:

```yaml
reports:
  - name: "carrier_a_sales"
    sheet: "Sales"
    sql_file: "sql/carrier_a_sales.sql"
```

---


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

## SQL macros

Macros are named SQL expressions registered at pipeline startup and available
in any SQL file — views, deliverables, or anywhere in DuckDB.

Use macros for transformations that are shared across multiple carriers,
such as normalising transaction types, standardising region codes, or
applying business logic that would otherwise be duplicated across SQL files.

**Scaffold a new macro:**

```bash
vp new-macro normalize_transaction_type
```

This creates a template file in your `macros/` directory and registers the
directory in `pipeline.yaml`:

```sql
-- macros/normalize_transaction_type.sql
CREATE OR REPLACE MACRO normalize_transaction_type(val) AS
    CASE
        WHEN val = 'Issuance' THEN 'Reinstatement'
        ELSE val
    END;
```

Edit the file with your logic. Macros are loaded at startup, so after
editing run `vp db-init` to re-register them.

**Use the macro in any SQL file:**

```sql
-- config/sql/carrier_a_sales.sql
SELECT
    order_id,
    normalize_transaction_type(transaction_type) AS transaction_type,
    price
FROM sales
WHERE order_date <= current_date
```

The same macro works in carrier B, C, or any view — write it once, use it
everywhere.

**Rules:**
- Use `CREATE OR REPLACE MACRO` so re-running `vp db-init` is idempotent.
- Macros are loaded in filename order. If one macro references another,
  name the dependency so it sorts first.
- The `macros_dir` path in `pipeline.yaml` defaults to `macros/`.

---

## Shared views

If multiple deliverables need the same transformation logic — standardising
region codes, zeroing out negatives, joining a lookup table — define it once
as a view in `config/views_config.yaml` and reference it by name in any SQL
file. Views produce intermediate tables; macros produce reusable expressions.
Use views for table-level transformations and macros for column-level ones.

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
    GREATEST(price, 0) AS price,
    order_date
FROM sales
```

Any deliverable SQL file can then reference the view like a table:

```sql
-- config/sql/carrier_a_sales.sql
SELECT order_id, price
FROM clean_sales
WHERE order_date >= date_trunc('month', current_date)
```

**Creation order matters** — if a view references another view, list the
dependency first in `views_config.yaml`.

Views are created by `vp db-init` and refreshed by `vp refresh-views`.
They are also refreshed automatically during `vp run-all`.

```bash
vp refresh-views
vp run-all --deliverable carrier_a_pack
```

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

Omitting `from:` or `to:` means no lower or upper bound.

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

## Carrier-specific pattern

Each carrier gets its own deliverable pointing to its own SQL file.
Shared reports can still be included in multiple deliverables without
duplication.

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
        filters:
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
- Output files are never auto-deleted. Re-running on the same date
  overwrites the file.
- `output_dir` on the deliverable overrides `pipeline.yaml`. If neither
  is set, defaults to `output/reports/`.