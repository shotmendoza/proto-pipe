# proto-pipe

A config-driven data validation and reporting pipeline backed by DuckDB.
Ingest CSV and Excel files, run checks, and produce deliverable reports —
all driven by YAML with no code changes required for new sources or reports.

---

## Installation

```bash
pip install proto-pipe
```

---

## Quickstart

```bash
# 1. Scaffold config files into your project
vp init

# 2. Edit pipeline.yaml to confirm your paths, then initialize DuckDB
vp db-init

# 3. Define your data sources interactively
vp new source

# 4. Drop files into data/incoming/ and ingest them
vp ingest

# 5. Define validation checks for a table
vp new report

# 6. Run checks
vp validate

# 7. Define and produce a deliverable
vp new deliverable
vp pull-report <deliverable-name>

# Or chain everything in one command
vp run-all --deliverable <deliverable-name>
```

Run `vp help` at any time to see the full workflow guide.

---

## CLI Reference

### Workflow

| Command              | Description                                    |
|----------------------|------------------------------------------------|
| `vp help`            | Show the end-to-end workflow guide             |
| `vp init`            | Scaffold config files into your project        |
| `vp db-init`         | Create DuckDB files and bootstrap tables       |
| `vp ingest`          | Load files from incoming directory into DuckDB |
| `vp validate`        | Run registered checks against ingested tables  |
| `vp pull-report <n>` | Query tables and write deliverable output      |
| `vp run-all`         | Chain ingest → validate → pull-report          |

### Scaffold new resources

| Command              | Description                          |
|----------------------|--------------------------------------|
| `vp new source`      | Define a data source interactively   |
| `vp new report`      | Define validation checks for a table |
| `vp new deliverable` | Define output format and recipients  |
| `vp new view`        | Scaffold a SQL view file             |
| `vp new macro`       | Scaffold a DuckDB macro file         |
| `vp new sql`         | Scaffold a deliverable SQL file      |

### Edit existing resources

| Command               | Description                            |
|-----------------------|----------------------------------------|
| `vp edit source`      | Update source configuration            |
| `vp edit report`      | Update report checks and params        |
| `vp edit deliverable` | Update deliverable settings            |
| `vp edit table`       | Open a table in the interactive editor |
| `vp edit column-type` | Edit declared column types             |

### Delete resources

| Command                 | Description                             |
|-------------------------|-----------------------------------------|
| `vp delete source`      | Remove source config and drop the table |
| `vp delete report`      | Remove report config                    |
| `vp delete deliverable` | Remove deliverable config               |
| `vp delete table`       | Drop a non-pipeline table               |

### Review & fix

| Command                   | Description                                   |
|---------------------------|-----------------------------------------------|
| `vp flagged`              | Browse ingest-time conflicts                  |
| `vp flagged edit`         | View conflicts with source data context       |
| `vp flagged clear`        | Clear flags without applying corrections      |
| `vp flagged retry <file>` | Apply corrected file and clear resolved flags |
| `vp validated`            | Browse check failures                         |
| `vp export-validation`    | Export check failures to Excel                |

### Explore

| Command               | Description                                |
|-----------------------|--------------------------------------------|
| `vp view source`      | Ingested rows with flag status column      |
| `vp view report`      | Source table for a report                  |
| `vp view deliverable` | Full deliverable output preview (no limit) |
| `vp view table`       | Any pipeline table                         |
| `vp table <name>`     | Quick table browse, edit, or export        |
| `vp funcs`            | Inspect registered check functions         |

### Config & setup

| Command                       | Description                                 |
|-------------------------------|---------------------------------------------|
| `vp config show`              | Print current path settings                 |
| `vp config set <key> <value>` | Update a path setting                       |
| `vp db-init --migrate`        | Apply pending schema migrations             |
| `vp refresh-views`            | Refresh DuckDB views from views_config.yaml |

---

## Project Structure

```
your-project/
├── pipeline.yaml                     # path defaults (created by vp init)
├── config/
│   ├── sources_config.yaml           # file naming conventions per source table
│   ├── reports_config.yaml           # checks, templates, reports
│   ├── deliverables_config.yaml      # output format and filters per deliverable
│   ├── views_config.yaml             # shared transformation views (optional)
│   └── sql/
│       ├── views/                    # SQL files for shared views
│       └── *.sql                     # SQL files for deliverable queries
├── data/
│   ├── incoming/                     # drop CSV/Excel files here
│   ├── pipeline.db                   # DuckDB — ingested tables + pipeline logs
│   └── watermarks.db                 # DuckDB — watermark tracking
└── output/
    └── reports/                      # deliverable output files
```

---

## Package Structure

```
src/proto_pipe/
  checks/         — CheckRegistry, built-ins, @custom_check decorator
  cli/
    commands/     — vp new, vp edit, vp delete, vp view, vp funcs, vp help
    flagged.py    — vp flagged group + vp validated
    prompts.py    — interactive wizards for source/report/deliverable setup
    quickstart.py — vp init, vp db-init, vp config
    reports.py    — vp pull-report, vp run-all, vp refresh-views
  io/             — DuckDB ops, file ingestion, config classes, migration
  pipelines/      — integrity pre-scan, watermark tracking
  reports/        — check runner, deliverable writer, validation flags
  constants.py
  main.py
```

---

## DuckDB Pipeline Tables

| Table                     | Description                                          |
|---------------------------|------------------------------------------------------|
| `<source_table>`          | One table per source in sources_config.yaml          |
| `ingest_log`              | Every file ingested, including failures and skips    |
| `flagged_rows`            | Ingest-time conflicts — duplicate keys, changed rows |
| `validation_flags`        | Check failures from `vp validate`                    |
| `check_registry_metadata` | Registered check metadata                            |
| `watermark`               | Last-processed timestamp per report                  |
| `report_runs`             | Record of every deliverable produced                 |

---

## Adding a Custom Check

```python
# my_checks.py
import pandas as pd
from proto_pipe.checks.helpers import custom_check

@custom_check("margin_check", kind="check")
def margin_check(col: str, threshold: float = 0.2) -> pd.Series:
    """Check that margin values are above the declared threshold."""
    def _run(context: dict) -> pd.Series:
        df = context["df"]
        return df[col] >= threshold
    return _run
```

Point `custom_checks_module` in `pipeline.yaml` at your file. Run `vp funcs`
to confirm the check registered correctly.

---

## Correcting Flagged Rows

```bash
# View conflicts for a table
vp flagged --table sales

# Open enriched editable view (requires textual)
vp flagged edit --table sales

# Export to CSV, fix values, re-import
vp flagged --table sales --export csv
# ... after editing the CSV
vp flagged retry flagged_sales_2026-04-01.csv --table sales
```

---

## Path Configuration

```bash
vp config set incoming_dir /Volumes/SharedDrive/data/incoming/
vp config set pipeline_db /mnt/shared/pipeline.db
vp config show
```

| Key                   | Default                           |
|-----------------------|-----------------------------------|
| `sources_config`      | `config/sources_config.yaml`      |
| `reports_config`      | `config/reports_config.yaml`      |
| `deliverables_config` | `config/deliverables_config.yaml` |
| `views_config`        | `config/views_config.yaml`        |
| `pipeline_db`         | `data/pipeline.db`                |
| `watermark_db`        | `data/watermarks.db`              |
| `incoming_dir`        | `data/incoming/`                  |
| `output_dir`          | `output/reports/`                 |
