# First Time Setup

This guide walks you through setting up the pipeline from scratch — from
installation to producing your first deliverable. It assumes you have Python
installed and access to the shared folder where your data files live.

---

## Terminology

Before diving in, here's what the pipeline means by each term:

| Term | What it is |
|---|---|
| **Source** | A raw file pulled from a data source — an unformatted CSV or Excel file sitting in a directory. Sources are defined in `sources_config.yaml` and loaded into DuckDB by `vp ingest`. |
| **Report** | A validated version of a source table. Checks run against reports to identify bad rows, which are flagged for review. Correcting flagged rows keeps the report tables in the database accurate. |
| **Deliverable** | A transformed, formatted version of one or more reports — shaped for a specific stakeholder with their expected column names and layout. Defined in `deliverables_config.yaml` and written to disk by `vp pull-report`. |

---

## 1. Install the pipeline

**With pip:**

```bash
pip install validation-pipeline
```

**With uv (recommended):**

```bash
uv add validation-pipeline
```

**From a GitHub repository (for development or internal forks):**

```bash
pip install git+https://github.com/your-org/validation-pipeline.git

# With uv
uv add git+https://github.com/your-org/validation-pipeline.git
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

Open `pipeline.yaml`. Each key is annotated with whether it needs to be
updated or can stay at its default. The most important one to change is
`incoming_dir` — point it at the folder where your source files arrive:

```bash
vp config set incoming_dir /Volumes/SharedDrive/data/incoming/
```

Or edit `pipeline.yaml` directly. All other defaults are fine to leave as-is
for a first setup.

The `macros_dir` key controls where SQL macro files live. Macros are
registered at startup and available in any deliverable or view SQL query.
The default is `macros/` in your project root — create it when you're ready
to add macros using `vp new-macro <name>`.

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
  [ok]   flagged_rows, validation_flags, and report_runs tables ready

Initialising watermark DB: data/watermarks.db
  [ok]   Watermark table ready
```

Safe to re-run — existing tables are never overwritten.

---

## 7. Drop your first file and ingest it

Copy or move a source file into your `incoming_dir`. Then run:

```bash
vp ingest
```

You'll see output for each file processed:

```
Ingesting from: data/incoming/

  [ok] 'sales_2026-03.csv' → 'sales' (1243 rows, created)

  1 loaded, 0 skipped, 0 failed — see ingest_log for details.
```

Source files that have already been successfully ingested are skipped
automatically on subsequent runs — you don't need to move or delete them.

If a file fails, it stays in the incoming folder and is retried on the next
run. To see the reason for any failure:

```bash
vp ingest-log --status failed
```

Any rows that arrive with a conflicting value for an existing record are
written to the `flagged_rows` table. These ingest conflicts must be resolved
before a deliverable can be produced. See `docs/reviewing_flagged_rows.md`.

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

Use `vp new-report` to add reports interactively — the wizard handles column
selection, alias_map setup, and param filling without editing YAML directly.

See `docs/adding_reports.md` for the full key reference and all available
built-in checks. See `docs/adding_checks.md` to write your own.

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

  ⚠  3 validation flag(s) across 1 report(s).
  Run: vp export-validation  to export a detail + summary report.
```

A `✓` means the check passed. A `✗` means it raised an error — the message
tells you why.

Any rows that fail a check are written to the `validation_flags` table.
Validation flags are **warnings only** — they do not block a deliverable from
being produced. Run `vp export-validation` to get a two-sheet Excel file
(Detail: one row per flagged record; Summary: one row per check) to review
what needs fixing. See `docs/reviewing_flagged_rows.md` for the full workflow.

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
to use SQL files for joins and carrier-specific column shaping.

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

Validation flags do not block this step. If there are open ingest conflicts
in `flagged_rows`, the deliverable is blocked until they are resolved (or
`--ignore-flagged` is passed).

---

## 12. Run everything at once

Once your config is set up, you can chain all steps in one command:

```bash
vp run-all --deliverable monthly_pack
```

This runs ingest → validate → refresh views → pull report in sequence.
If ingest conflicts exist in `flagged_rows`, it stops before producing the
deliverable. Validation flags in `validation_flags` print a warning but do
not stop the run. Pass `--ignore-flagged` to bypass ingest conflict blocking.

---

## Day-to-day workflow

Once set up, your daily or monthly run is just:

```bash
vp run-all --deliverable monthly_pack
```

New files dropped into the incoming folder are picked up automatically.
Files already ingested are skipped. The deliverable is written to the output
folder with the current date in the filename.

If the run stops due to ingest conflicts:

```bash
vp flagged-summary                                              # see what needs attention
vp export-flagged --table sales                                 # export for correction
vp import-corrections flagged_sales_2026-03-26.csv --table sales  # apply fixes
vp run-all --deliverable monthly_pack                           # re-run clean
```

If the run completes but prints a validation warning:

```bash
vp export-validation          # export Detail + Summary sheets
                              # fix records at the source, re-ingest, re-validate
```

---

## Quick reference

| Task                                   | Command                                    |
|----------------------------------------|--------------------------------------------|
| First-time setup                       | `vp init` then `vp db-init`                |
| Load new source files                  | `vp ingest`                                |
| Check why a file failed to load        | `vp ingest-log --status failed`            |
| Run checks                             | `vp validate`                              |
| Produce a deliverable                  | `vp pull-report <n>`                       |
| Run everything                         | `vp run-all --deliverable <n>`             |
| Check current paths                    | `vp config show`                           |
| Update a path                          | `vp config set <key> <value>`              |
| List available checks                  | `vp checks`                                |
| Scaffold a new source                  | `vp new-source`                            |
| Scaffold a new report                  | `vp new-report`                            |
| Scaffold a new deliverable             | `vp new-deliverable`                       |
| Scaffold a SQL query file              | `vp new-sql <n>`                           |
| Scaffold a macro                       | `vp new-macro <n>`                         |
| Reset a report table                   | `vp table-reset --report <n>`              |
| Review ingest conflicts                | `vp flagged-summary`                       |
| View ingest conflict rows              | `vp flagged-list --table <n>`              |
| Export ingest conflicts for correction | `vp export-flagged --table <n>`            |
| Apply corrections                      | `vp import-corrections <file> --table <n>` |
| Clear ingest conflicts                 | `vp flagged-clear --table <n>`             |
| Export validation flags for review     | `vp export-validation`                     |
| Refresh views manually                 | `vp refresh-views`                         |
| Review ingest conflict workflow        | See `docs/reviewing_flagged_rows.md`       |
| Write a custom check                   | See `docs/adding_checks.md`                |