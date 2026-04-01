# First Time Setup

This guide walks you through setting up the pipeline from scratch — from
installation to producing your first deliverable.

---

## 1. Install the pipeline

```bash
pip install proto-pipe
```

```bash
uv add "git+https://github.com/shotmendoza/proto-pipe
```


Verify it installed correctly:

```bash
vp --help
```

You should see a list of command groups. Run `vp help` for a workflow-oriented
guide showing the steps from source setup to deliverable export.

---

## 2. Create your project folder

```bash
mkdir my-pipeline
cd my-pipeline
```

---

## 3. Scaffold the config files

```bash
vp init
```

This creates:

```
my-pipeline/
├── pipeline.yaml              # path defaults — edit this first
└── config/
    ├── sources_config.yaml
    ├── reports_config.yaml
    ├── deliverables_config.yaml
    └── views_config.yaml
```

---

## 4. Set your paths in pipeline.yaml

Open `pipeline.yaml`. The most important path to update is `incoming_dir` —
point it at the folder where your data files arrive:

```bash
vp config set incoming_dir /Volumes/SharedDrive/data/incoming/
```

All other defaults are fine to leave as-is for a first setup. Check current
settings at any time with:

```bash
vp config show
```

---

## 5. Initialize the database

```bash
vp db-init
```

Creates `data/pipeline.db` and `data/watermarks.db` and bootstraps the
pipeline infrastructure tables. Safe to re-run — existing tables are never
overwritten. Pass `--migrate` to apply schema changes on an existing DB.

---

## 6. Define your first source

```bash
vp new source
```

The wizard walks you through:
- Picking a file from the incoming directory
- Setting the file pattern (e.g. `sales_*.csv`)
- Choosing the primary key column
- Confirming column types

This writes an entry to `config/sources_config.yaml`.

---

## 7. Ingest your first file

Drop a matching file into your `incoming_dir`, then run:

```bash
vp ingest
```

```
[ok] 'sales_2026-03.csv' → 'sales' (1243 rows, created)
```

Files already ingested are skipped automatically on subsequent runs.
To see ingest history:

```bash
vp view table ingest_log
```

---

## 8. Define validation checks

```bash
vp new report
```

The wizard selects the table you just ingested, lets you name the report,
then presents all available checks with their docstrings inline. Column
params offer a selector from the table's columns. Press ESC at any step
to go back.

After completing, run:

```bash
vp validate
```

To view check failures:

```bash
vp validated
vp export-validation
```

---

## 9. Define a deliverable

```bash
vp new deliverable
```

Deliverables define what gets written to disk — which reports to include,
format (CSV or Excel), and filename template. The wizard scaffolds a SQL
file for you to customise with joins and column selection.

---

## 10. Produce the deliverable

```bash
vp pull-report <deliverable-name>
```

Output is written to `output/reports/`.

---

## 11. Run everything at once

Once your config is set up, the daily run is:

```bash
vp run-all --deliverable <deliverable-name>
```

Runs ingest → validate → pull-report in sequence. Stops before producing
the deliverable if ingest conflicts exist (rows flagged in `flagged_rows`).
Pass `--ignore-flagged` to produce the deliverable anyway.

---

## Reviewing and fixing flagged rows

Ingest conflicts are rows that arrived with changed values for an existing
primary key. Review and correct them:

```bash
# Browse flagged rows in the terminal
vp flagged --table sales

# Open an enriched editable view (requires textual)
vp flagged edit --table sales

# Export to CSV, fix, re-import
vp flagged --table sales --export csv
# ... edit the CSV ...
vp flagged retry flagged_sales_2026-04-01.csv --table sales
```

See `docs/reviewing_flagged_rows.md` for more detail.

---

## Quick reference

| Task                    | Command                        |
|-------------------------|--------------------------------|
| First-time setup        | `vp init` then `vp db-init`    |
| Define a source         | `vp new source`                |
| Load new files          | `vp ingest`                    |
| Run checks              | `vp validate`                  |
| Produce a deliverable   | `vp pull-report <n>`           |
| Run everything          | `vp run-all --deliverable <n>` |
| Workflow guide          | `vp help`                      |
| Check current paths     | `vp config show`               |
| Inspect check functions | `vp funcs`                     |
| Browse any table        | `vp table <n>`                 |
| Review ingest conflicts | `vp flagged --table <n>`       |
| Review check failures   | `vp validated`                 |