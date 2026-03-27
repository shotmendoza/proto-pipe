# Reviewing Flagged Rows

When validation runs or a new file is ingested, rows that fail a check or
conflict with existing data are written to the `flagged_rows` table.
This guide explains how to find them, understand what went wrong, decide
what to do, and apply corrections.

---

## Where flagged rows come from

Rows get flagged from two places:

**During validation** (`vp validate` or `vp run-all`) — checks defined in
`reports_config.yaml` run against ingested data. If a check raises, the
offending rows are written to `flagged_rows` with the check name and reason.

**During ingest** (`vp ingest`) — when `on_duplicate: flag` is set on a
source in `sources_config.yaml`, the pipeline computes a checksum for each
incoming row and compares it to the existing row with the same primary key.
If the content differs, the incoming row is flagged and skipped — the original
row stays in the table until you resolve the conflict.

---

## Quick health check

```bash
vp flagged-summary
```

This shows a count of open flags by table, check, and reason:

```
  14 flagged row(s) across 2 table(s):

  sales
       9  [duplicate_conflict]  row content changed: price, region
       3  [price_range_check]   price out of range (min: 0, max: 500)
  inventory
       2  [null_check]          null values in required columns
```

Run this after every ingest or validate to know immediately whether anything
needs attention before producing deliverables.

---

## Viewing flagged rows in the terminal

```bash
vp flagged-list --table sales
```

Prints each flagged row inline with its source data and flag reason:

```
  12 flagged row(s) for 'sales' (showing 50):

  ── [duplicate_conflict] row content changed: price, region
     flagged: 2026-03-26 09:14:22+00:00
     order_id: ORD-042
     price: -50.0
     region: EMEA
     ...
```

Filter by a specific check:

```bash
vp flagged-list --table sales --check duplicate_conflict
vp flagged-list --table sales --check price_range_check
```

Limit how many rows are shown (default 50):

```bash
vp flagged-list --table sales --limit 20
```

---

## Deciding what to do

**Option A — The data is wrong and needs to be corrected.**
Export the flagged rows, fix the values, and import them back.
See the correction workflow below.

**Option B — The check is too strict.**
The data is actually fine but the check params are wrong — for example, the
price range was set too narrow. Update the check in `reports_config.yaml`
and re-run. The flags won't reappear for those rows.

**Option C — The conflict is known and acceptable.**
You've reviewed the flags and want to proceed without correcting. Clear them:

```bash
vp flagged-clear --table sales
vp flagged-clear --table sales --check duplicate_conflict   # clear one check only
```

This asks for confirmation before clearing. Pass `--yes` to skip the prompt:

```bash
vp flagged-clear --table sales --yes
```

**Option D — Proceed anyway.**
Produce the deliverable regardless of open flags:

```bash
vp run-all --deliverable monthly_pack --ignore-flagged
```

Flags remain in `flagged_rows` — they are not cleared by `--ignore-flagged`.

---

## Duplicate conflict flags

When `on_duplicate: flag` is set in `sources_config.yaml`, the pipeline
detects when a new file contains a row whose primary key already exists but
whose content has changed. The incoming row is skipped and a flag is written
with the names of the columns that changed:

```
[duplicate_conflict]  row content changed: price, region
```

This means a row with the same `order_id` arrived with different `price`
and `region` values. The original row is preserved. You decide which version
is correct via the correction workflow.

To re-check an existing table for conflicts without waiting for a new file:

```bash
vp check-null-overwrites --table sales
```

---

## Correcting flagged rows

### Step 1 — Export to CSV

```bash
vp export-flagged --table sales
```

Writes to `output/reports/flagged_sales_YYYY-MM-DD.csv`. To choose a path:

```bash
vp export-flagged --table sales --output /tmp/sales_fixes.csv
```

The file contains all source columns plus two annotation columns at the front:

| Column | Purpose |
|---|---|
| `_flag_id` | Internal ID — **do not delete or edit this column** |
| `_flag_reason` | Why the row was flagged — for reference only |

### Step 2 — Fix the values

Open the CSV, edit the data columns that are wrong, and save. Leave
`_flag_id` and `_flag_reason` exactly as they are.

### Step 3 — Import the corrections

```bash
vp import-corrections output/reports/flagged_sales_2026-03-26.csv --table sales
```

The pipeline updates the matching rows by primary key and clears the
resolved flags:

```
  [ok] 2 row(s) updated in 'sales'
  [ok] 2 flag(s) cleared from flagged_rows
```

Override the primary key if it's not defined in `sources_config.yaml`:

```bash
vp import-corrections flagged_sales.csv --table sales --key order_id
```

---

## After correcting

Re-run validation to confirm the fixed rows now pass:

```bash
vp validate
```

Then check the summary is clear:

```bash
vp flagged-summary
```

If all clear, produce the deliverable:

```bash
vp pull-report monthly_pack
```

---

## Common scenarios

**"The same row keeps getting flagged after every ingest."**
The source system is consistently sending a different value for that row.
Decide which version is correct, apply the correction once, then consider
switching `on_duplicate` to `upsert` for that source if you always want the
latest value to win.

**"I corrected the rows but they got flagged again."**
A new file arrived with the original bad values. This means the problem is
in the source system. Fix it at the source if possible, or apply corrections
again. If you always expect certain fields to change, switch those sources to
`on_duplicate: upsert`.

**"I want to clear all flags without correcting anything."**

```bash
vp flagged-clear --table sales --yes
```

**"I don't have a primary key column in my data."**
Without a primary key, conflict detection and the correction workflow are
not available. Raise this with whoever provides the source files, or define
a synthetic key and add it during ingestion.

**"How do I see what on_duplicate is set to for a table?"**

```bash
vp config show
```

Or open `config/sources_config.yaml` directly. The `on_duplicate` field
is defined per source.