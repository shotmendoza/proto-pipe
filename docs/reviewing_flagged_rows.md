# Reviewing Flagged Rows

The pipeline maintains two separate flag tables with different purposes,
different correction workflows, and different effects on deliverables.

| Table | Populated by | Blocks deliverable? | Correction path |
|---|---|---|---|
| `flagged_rows` | Ingest — rows that conflict with existing records | **Yes** | Export CSV → fix → import-corrections |
| `validation_flags` | Validation — rows that fail a check | **No** (warning only) | Fix at source → re-ingest → re-validate |

---

## Ingest conflicts (`flagged_rows`)

### Where they come from

When `on_duplicate: flag` is set on a source in `sources_config.yaml`, the
pipeline computes a checksum for each incoming row and compares it to the
existing row with the same primary key. If the content differs, the incoming
row is skipped and a conflict flag is written — the original row stays in the
table until you resolve it.

### Quick health check

```bash
vp flagged-summary
```

This shows a count of open ingest conflicts by table and reason:

```
  9 flagged row(s) across 1 table(s):

  sales
       9  [duplicate_conflict]  price: 99.99 -> -50.0 | region: EMEA -> APAC
```

Run this after every ingest to know immediately whether anything needs
attention before producing deliverables.

### Viewing conflicts in the terminal

```bash
vp flagged-list --table sales
```

Prints each flagged row inline with its source data and conflict reason:

```
  9 flagged row(s) for 'sales' (showing 50):

  ── [duplicate_conflict] price: 99.99 -> -50.0 | region: EMEA -> APAC
     flagged: 2026-03-26 09:14:22+00:00
     order_id: ORD-042
     price: 99.99
     region: EMEA
     ...
```

Filter by check name or limit rows shown:

```bash
vp flagged-list --table sales --check duplicate_conflict
vp flagged-list --table sales --limit 20
```

### Deciding what to do

**Option A — The incoming data is correct and should replace the existing row.**
Export, fix, and import back via the correction workflow below.

**Option B — The existing data is correct and the incoming row should be ignored.**
Clear the flag — the original row stays as-is:

```bash
vp flagged-clear --table sales
vp flagged-clear --table sales --check duplicate_conflict   # clear one check only
```

Pass `--yes` to skip the confirmation prompt.

**Option C — Proceed with the deliverable anyway.**

```bash
vp run-all --deliverable monthly_pack --ignore-flagged
```

Flags remain in `flagged_rows` — they are not cleared by `--ignore-flagged`.

### Correcting ingest conflicts

**Step 1 — Export to CSV:**

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

**Step 2 — Fix the values.** Open the CSV, edit the data columns that are
wrong, and save. Leave `_flag_id` and `_flag_reason` exactly as they are.

**Step 3 — Import the corrections:**

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

**Step 4 — Re-run validation and produce the deliverable:**

```bash
vp validate
vp flagged-summary       # confirm all clear
vp pull-report monthly_pack
```

### Scanning for conflicts manually

To re-check an existing table for conflicts without waiting for a new ingest:

```bash
vp check-null-overwrites --table sales
```

---

## Validation flags (`validation_flags`)

### Where they come from

When `vp validate` runs, checks defined in `reports_config.yaml` are applied
to each report's watermark-filtered rows. Any rows that fail a check are
written to `validation_flags` — one entry per failing row when the check
returns a boolean mask, or one summary entry per check run otherwise.

Validation flags are **warnings only**. They do not block a deliverable from
being produced. The deliverable is always written when `vp run-all` or
`vp pull-report` is called, regardless of open validation flags.

### Reviewing validation flags

After `vp validate`, the terminal prints a summary:

```
  ⚠  3 validation flag(s) across 1 report(s).
  Run: vp export-validation  to export a detail + summary report.
```

Export a two-sheet Excel file to review the detail:

```bash
vp export-validation                              # all reports
vp export-validation --report sales_validation    # one report only
vp export-validation --output /tmp/review.xlsx    # custom output path
```

The exported file has two sheets:

- **Detail** — one row per flagged record. The primary key column is named
  after the actual column from your source (e.g. `order_id`, `internal_id`)
  so you can trace each record back immediately.
- **Summary** — one row per check, with total failure counts and timestamps.

### Correction path

Validation flags identify records in the report table whose data is wrong.
The correction workflow mirrors the ingest conflict workflow — export, fix,
import back:

**Step 1 — Export to Excel:**

```bash
vp export-validation                              # all reports
vp export-validation --report sales_validation    # one report only
vp export-validation --output /tmp/review.xlsx    # custom path
```

The exported file has two sheets:

- **Detail** — one row per flagged record. The first column is `_flag_id` —
  do not edit or delete it. The primary key column is named after your actual
  column (e.g. `order_id`). Fix the data values in this sheet.
- **Summary** — one row per check with total failure counts. Reference only.

**Step 2 — Fix the values.** Open the Detail sheet, correct the data columns
that are wrong, and save the file. Leave `_flag_id` exactly as-is.

**Step 3 — Import the corrections:**

```bash
vp import-corrections output/reports/validation_sales_2026-03-26.xlsx --table sales
```

The pipeline applies the corrected values directly to the report table by
primary key, then clears the resolved entries from `validation_flags`:

```
  [ok] 2 row(s) updated in 'sales'
  [ok] 2 validation flag(s) cleared from validation_flags
```

**Step 4 — Re-validate to confirm:**

```bash
vp validate
vp export-validation   # confirm no remaining flags
```

The watermark means only the corrected rows (and any newer rows) will be
re-checked — not the entire table.

> **Note:** `vp import-corrections` works for both ingest conflicts and
> validation flags. The `_flag_id` column in the file determines which table
> is cleared — it does not matter which export command produced the file.

---

## Common scenarios

**"The same row keeps being flagged as a duplicate conflict after every ingest."**
The source system is consistently sending a different value for that row.
Decide which version is correct, apply the correction once, then consider
switching `on_duplicate` to `upsert` for that source if you always want the
latest value to win.

**"I corrected a row but it got flagged again."**
A new file arrived with the original bad values. The problem is in the source
system. Fix it at the source if possible, or set `on_duplicate: upsert` to
always accept the incoming value.

**"I want to clear all ingest conflicts without correcting anything."**

```bash
vp flagged-clear --table sales --yes
```

**"Validation flags keep appearing for the same records."**
The check params may be too strict, or the source data has a systematic
quality issue. If the params are wrong, update the check in
`reports_config.yaml`. If the data is the issue, work with whoever provides
the source files to fix it upstream.

**"I don't have a primary key column in my data."**
Without a primary key, ingest conflict detection and the export/correction
workflow are not available. Define a synthetic key and add it during
ingestion, or raise it with whoever provides the source files.

**"How do I see what on_duplicate is set to for a table?"**

Open `config/sources_config.yaml` directly, or run `vp config show`.