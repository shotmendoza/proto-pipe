# First Time Setup

This guide walks you through setting up the pipeline from scratch — from
installation to producing your first deliverable. It assumes you have Python
installed and access to the shared folder where your data files live.

---

## 1. Install the pipeline

```bash
pip install validation-pipeline
```

Verify it installed correctly:

```bash
vp --help
```

You should see a list of available commands. If you see a "command not found"
error, make sure your Python environment's `bin` folder is on your PATH.

---

## 2. Create your project folder

Make a folder for your project and navigate into it. Everything — configs,
data, and output — will live here.

```bash
mkdir my-pipeline
cd my-pipeline
```

---

## 3. Scaffold the config files

```bash
vp init
```

This creates the following structure:

```
my-pipeline/
├── pipeline.yaml              # path defaults — edit this first
└── config/
    ├── sources_config.yaml    # define your data files here
    ├── reports_config.yaml    # define your validation checks here
    ├── deliverables_config.yaml  # define your output files here
    └── views_config.yaml      # shared transformations (optional for now)
```

---

## 4. Set your paths in pipeline.yaml

Open `pipeline.yaml`. By default it looks like this:

```yaml
paths:
  sources_config:       "config/sources_config.yaml"
  reports_config:       "config/reports_config.yaml"
  deliverables_config:  "config/deliverables_config.yaml"
  views_config:         "config/views_config.yaml"
  pipeline_db:          "data/pipeline.db"
  watermark_db:         "data/watermarks.db"
  incoming_dir:         "data/incoming/"
  output_dir:           "output/reports/"
```

The most important path to update is `incoming_dir` — point it at the shared
folder where your data files arrive:

```bash
vp config set incoming_dir /Volumes/SharedDrive/data/incoming/
```

Or edit `pipeline.yaml` directly. All other defaults are fine to leave as-is
for a first setup.

---

## 5. Define your sources

Open `config/sources_config.yaml`. A source tells the pipeline what files to
look for, what table to load them into, and what column tracks when a row
was last updated.

For each type of file you receive, add an entry:

```yaml
sources:
  - name: "sales"
    patterns:
      - "sales_*.csv"        # matches sales_2026-03.csv, sales_jan.csv, etc.
      - "Sales_*.xlsx"       # same source, different naming convention
    target_table: "sales"
    timestamp_col: "updated_at"
    primary_key: "order_id"  # used if you need to correct flagged rows later
```

**Pattern tips:**
- `*` matches any sequence of characters
- List every naming variation you expect — files that don't match any pattern
  are logged as unmatched and skipped
- Patterns are case-sensitive, so `sales_*.csv` and `Sales_*.csv` are different

---

## 6. Initialise the database

```bash
vp db-init
```

This creates `data/pipeline.db` and `data/watermarks.db`, and bootstraps an
empty table for each source you defined. You'll see output like:

```
Initialising pipeline DB: data/pipeline.db
  [ok]   Created table 'sales'
  [ok]   flagged_rows and report_runs tables ready

Initialising watermark DB: data/watermarks.db
  [ok]   Watermark table ready
```

Safe to re-run — existing tables are never overwritten.

---

## 7. Drop your first file and ingest it

Copy or move a data file into your `incoming_dir`. Then run:

```bash
vp ingest
```

You'll see output for each file processed:

```
Ingesting from: data/incoming/

  [ok] 'sales_2026-03.csv' → 'sales' (1243 rows, created)

  1 loaded, 0 skipped, 0 failed — see ingest_log for details.
```

Files that have already been successfully ingested are skipped automatically
on subsequent runs — you don't need to move or delete them from the folder.

If a file fails, it stays in the incoming folder and is retried on the next
run. Check the reason with:

```sql
SELECT filename, status, message FROM ingest_log ORDER BY ingested_at DESC;
```

---

## 8. Define your validation checks

Open `config/reports_config.yaml`. Reports define what to validate and which
checks to run against each table. A minimal example:

```yaml
templates:
  standard_null_check:
    name: null_check

  price_range_check:
    name: range_check
    params:
      col: "price"
      min_val: 0
      max_val: 500

reports:
  - name: "sales_validation"
    source:
      type: duckdb
      path: "data/pipeline.db"
      table: "sales"
      timestamp_col: "updated_at"
    checks:
      - template: standard_null_check
      - template: price_range_check
```

See `docs/adding_reports.md` for the full key reference and all available
built-in checks.

---

## 9. Run validation

```bash
vp validate
```

Results are printed per report and per check:

```
Running validation across 1 report(s)...

  sales_validation [completed]
    ✓ standard_null_check
    ✗ price_range_check
      Column 'price' not found in DataFrame
```

A `✓` means the check returned without raising. A `✗` means it raised an
exception — the error message tells you why.

If checks flag bad rows, they are written to the `flagged_rows` table.
See `docs/reviewing_flagged_rows.md` for how to review and correct them.

---

## 10. Define your deliverable

Open `config/deliverables_config.yaml`. A deliverable defines what gets
written to disk — which reports to include, what format, and what filters
to apply.

A simple example producing one Excel file with two sheets:

```yaml
deliverables:
  - name: "monthly_pack"
    format: xlsx
    filename_template: "monthly_pack_{date}.xlsx"
    reports:
      - name: "sales_validation"
        sheet: "Sales"
        filters:
          date_filters:
            - col: "order_date"
              to: "end_of_last_month"
```

See `docs/adding_deliverables.md` for the full key reference, including how
to use SQL files for joins and carrier-specific logic.

---

## 11. Produce the deliverable

```bash
vp pull-report monthly_pack
```

Output is written to `output/reports/` (or the `output_dir` you configured):

```
  [query] sales_validation → 1243 rows
  [ok] output/reports/monthly_pack_2026-03-26.xlsx (1243 total rows)
```

---

## 12. Run everything at once

Once your config is set up, you can chain all steps in one command:

```bash
vp run-all --deliverable monthly_pack
```

This runs ingest → validate → refresh views → pull report in sequence.
If flagged rows exist, it stops before producing the deliverable and tells
you how many rows need review. Pass `--ignore-flagged` to produce the
deliverable anyway.

---

## Day-to-day workflow

Once set up, your daily or monthly run is just:

```bash
vp run-all --deliverable monthly_pack
```

New files dropped into the incoming folder are picked up automatically.
Files already ingested are skipped. The deliverable is written to the output
folder with the current date in the filename.

---

## Quick reference

| Task | Command |
|---|---|
| First-time setup | `vp init` then `vp db-init` |
| Load new files | `vp ingest` |
| Run checks | `vp validate` |
| Produce a deliverable | `vp pull-report <name>` |
| Run everything | `vp run-all --deliverable <name>` |
| Check current paths | `vp config show` |
| Update a path | `vp config set <key> <value>` |
| List available checks | `vp checks` |
| Review failed ingests | Query `ingest_log` in pipeline.db |
| Review flagged rows | See `docs/reviewing_flagged_rows.md` |
