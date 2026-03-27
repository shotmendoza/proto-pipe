# Reviewing Flagged Rows

When validation runs, rows that fail a check are written to the `flagged_rows`
table in `pipeline.db`. This guide explains how to find them, understand what
went wrong, decide what to do, and apply corrections.

***

## Where flagged rows come from

Each time you run `vp validate` or `vp run-all`, the pipeline runs your
defined checks against newly ingested data. If a check raises an error for
specific rows, those rows are recorded in `flagged_rows` with the reason.

Common reasons rows get flagged:
- A price or quantity value is outside the expected range
- A required column contains nulls
- A duplicate row was detected
- A value doesn't match expected categories

***

## Checking if you have flagged rows

After running validation, the CLI tells you directly:

```
⚠ 14 flagged row(s) require review before producing deliverables.
  Use: vp export-flagged --table <n>  to export them for correction.
  Re-run with --ignore-flagged to produce deliverables anyway.
```

You can also query the table directly in any SQL tool connected to
`data/pipeline.db`:

```sql
SELECT count(*) FROM flagged_rows;
```

***

## Understanding the flagged_rows table

```sql
SELECT * FROM flagged_rows ORDER BY flagged_at DESC;
```

| Column | What it means |
|---|---|
| `id` | Unique ID for this flag entry |
| `table_name` | Which source table the row came from (e.g. `sales`) |
| `row_index` | The row's position in the table at the time it was flagged |
| `check_name` | Which check caught it (e.g. `price_range_check`) |
| `reason` | The error message from the check — tells you exactly what was wrong |
| `flagged_at` | When the row was flagged |

To see flagged rows for a specific table:

```sql
SELECT * FROM flagged_rows WHERE table_name = 'sales';
```

To see a breakdown by check:

```sql
SELECT check_name, count(*) AS flagged_count
FROM flagged_rows
GROUP BY check_name
ORDER BY flagged_count DESC;
```

***

## Deciding what to do

You have three options depending on what you find:

**Option A — The data is wrong and needs to be corrected.**
The source file contained bad values — a negative price, a missing field, a
typo. You need to fix the values and write them back. Use the correction
workflow below.

**Option B — The check is too strict.**
The data is actually fine but your check parameters are wrong — for example,
the price range was set too narrow and valid prices are being flagged. In this
case, update the check params in `reports_config.yaml` and re-run validation.
The flags will no longer appear for those rows.

**Option C — The data is known to be imperfect and you want to proceed anyway.**
You're aware of the issues and need the deliverable regardless. Run:

```bash
vp run-all --deliverable <name> --ignore-flagged
```

This produces the deliverable without clearing the flags — the rows remain
in `flagged_rows` for the record.

***

## Correcting flagged rows

### Step 1 — Export the flagged rows to CSV

```bash
vp export-flagged --table sales
```

This writes a CSV to your `output_dir` (default: `output/reports/`) named
`flagged_sales.csv`. To choose a different location:

```bash
vp export-flagged --table sales --output /tmp/sales_corrections.csv
```

The file contains all columns from the source table plus two annotation
columns at the front:

| Column | Purpose |
|---|---|
| `_flag_id` | Internal ID — **do not delete this column** |
| `_flag_reason` | Why the row was flagged — read-only, for your reference |

Example export:

```
_flag_id, _flag_reason,          order_id, price,  region, order_date
abc-123,  price out of range,    ORD-042,  -50.0,  EMEA,   2026-01-15
def-456,  null value in price,   ORD-089,  ,       APAC,   2026-02-20
```

### Step 2 — Fix the values

Open the CSV in Excel, Google Sheets, or any editor. Fix the values in the
data columns. Leave `_flag_id` and `_flag_reason` exactly as they are — they
are used to match your corrections back to the right rows.

```
_flag_id, _flag_reason,          order_id, price,  region, order_date
abc-123,  price out of range,    ORD-042,  50.0,   EMEA,   2026-01-15
def-456,  null value in price,   ORD-089,  120.0,  APAC,   2026-02-20
```

Only edit the columns that are wrong. You don't need to fill in every column —
the pipeline only updates columns that are present in your file.

### Step 3 — Import the corrections

```bash
vp import-corrections output/reports/flagged_sales.csv --table sales
```

The pipeline will:
1. Match each row to the source table by the primary key defined in
   `sources_config.yaml` (e.g. `order_id`)
2. Update only the columns you changed
3. Remove the corrected entries from `flagged_rows`

You'll see confirmation:

```
  [ok] 2 row(s) updated in 'sales'
  [ok] 2 flag(s) cleared from flagged_rows
```

### Overriding the primary key

If your source doesn't have a `primary_key` defined in `sources_config.yaml`,
you can specify it directly:

```bash
vp import-corrections flagged_sales.csv --table sales --key order_id
```

***

## After correcting

Once corrections are imported, re-run validation to confirm the fixed rows
now pass:

```bash
vp validate
```

If `flagged_rows` is empty, you're clear to produce the deliverable:

```bash
vp pull-report monthly_pack
```

Or run everything together:

```bash
vp run-all --deliverable monthly_pack
```

***

## Checking correction history

Every time you run `import-corrections`, the rows are updated directly in
the source table. You can verify the current state of any row by querying
the table directly:

```sql
SELECT * FROM sales WHERE order_id = 'ORD-042';
```

To see how many flags are still open vs resolved:

```sql
-- Currently open flags
SELECT table_name, check_name, count(*) AS open_flags
FROM flagged_rows
GROUP BY table_name, check_name;

-- All rows delivered, with row counts and timestamps
SELECT deliverable_name, report_name, row_count, created_at
FROM report_runs
ORDER BY created_at DESC;
```

***

## Common scenarios

**"I exported the flagged rows but some look fine to me."**
The check may be misconfigured. Check the `reason` column to understand
what the check expected vs what it found. If the data is correct, update
the check parameters in `reports_config.yaml`.

**"I corrected the rows but they got flagged again on the next run."**
The source file was re-ingested with the original bad values. Since the
pipeline skips already-ingested files by filename, this usually means a new
file arrived with the same bad data. Fix it at the source if possible, or
apply corrections again after each ingest.

**"I don't have a primary key column in my data."**
Without a primary key, the pipeline can't match corrected rows back to the
right records. If your data doesn't have a natural unique identifier, raise
this with whoever provides the source files — or add a synthetic key during
ingestion as a custom check.

**"I want to clear all flags without correcting anything."**
This should be rare, but if needed you can do it directly in DuckDB:

```sql
DELETE FROM flagged_rows WHERE table_name = 'sales';
```

Only do this if you're certain the flagged rows don't need correction —
this action cannot be undone.
