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
# 1. Scaffold config files and initialize the database
vp init

# Or run each step separately (recommended)
vp init config
vp init db

# 2. Create a new data source
vp new source

# 3. Drop files into your incoming folder (set in config) and ingest them
vp ingest

# 4. Create a report
vp new report

# 5. Run checks, validations, and transformations on the report
vp validate

# 6. Create and produce a deliverable
vp new deliverable
vp deliver <deliverable-name>

# Or chain everything in one command
vp run-all --deliverable <deliverable-name>
```

Run `vp help` at any time to see the full workflow guide.

---

## CLI Reference

### Pipeline

| Command              | Description                                    |
|----------------------|------------------------------------------------|
| `vp ingest`          | Load files from incoming directory into DuckDB |
| `vp validate`        | Run registered checks against ingested tables  |
| `vp deliver <n>`     | Query tables and write deliverable output      |
| `vp run-all`         | Chain ingest → validate → deliver              |

### Inspect and Validate

| Command                              | Description                               |
|--------------------------------------|-------------------------------------------|
| `vp status [source / report] <name>` | Pipeline health, drill down to detail     |
| `vp errors [source / report] <name>` | View blocked records, drill down          |
| `vp errors <stage> export`           | Export to CSV; `--open` launches          |
| `vp errors <stage> edit`             | TUI inline edit                           |
| `vp errors <stage> clear`            | Drop without fixing; `--yes` skips prompt |
| `vp errors <stage> retry`            | Re-process corrected export               |

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

### Explore

| Command               | Description                                |
|-----------------------|--------------------------------------------|
| `vp view source`      | Ingested rows with flag status column      |
| `vp view report`      | Source table for a report                  |
| `vp view deliverable` | Full deliverable output preview (no limit) |
| `vp view table <n>`   | Any pipeline table                         |
| `vp funcs`            | Inspect registered check functions         |

### Config & setup

| Command                       | Description                                 |
|-------------------------------|---------------------------------------------|
| `vp help`                     | Show the end-to-end workflow guide          |
| `vp config show`              | Print current path settings                 |
| `vp config set <key> <value>` | Update a path setting                       |
| `vp init config`              | Scaffold config files into your project     |
| `vp init db`                  | Create DuckDB files and bootstrap tables    |
| `vp init db --migrate`        | Apply pending schema migrations             |
| `vp refresh views`            | Refresh DuckDB views from views_config.yaml |

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
    errors.py     — vp errors group (source + report)
    status.py     — vp status
    prompts.py    — interactive wizards for source/report/deliverable setup
    quickstart.py — vp init config, vp init db, vp config
    reports.py    — vp deliver, vp run-all, vp refresh views
  io/             — DuckDB ops, file ingestion, config classes, migration
  macros/         — SQL and Python macro loading
  pipelines/      — integrity pre-scan, watermark tracking
  reports/        — check runner, deliverable writer, validation flags
  constants.py
  main.py
```

---

## DuckDB Pipeline Tables

| Table                     | Description                                       |
|---------------------------|---------------------------------------------------|
| `<source_table>`          | One table per source in sources_config.yaml       |
| `ingest_state`            | Every file ingested, including failures and skips |
| `source_block`            | Ingest-time conflicts that blocks deliverables.   |
| `validation_block`        | Check failures from `vp validate`                 |
| `check_registry_metadata` | Registered check metadata                         |
| `watermark`               | Last-processed timestamp per report               |
| `pipeline_events`         | Record of every deliverable produced              |

---

## Adding a Custom Check

```python
# my_checks.py
import pandas as pd
from proto_pipe.checks.helpers import custom_check

@custom_check("margin_check", kind="check")
def margin_check(col: pd.Series, threshold: float = 0.2) -> pd.Series[bool]:
    """Check that margin values are above the declared threshold."""
    return col >= threshold
```

Point `custom_checks_module` in `pipeline.yaml` at your file. Run `vp funcs`
to confirm the check registered correctly.

---

## Correcting Flagged Rows

```bash
# View conflicts for a table
vp errors source sales

# Open enriched editable view (requires textual)
vp errors source edit sales

# Export to CSV, fix values, re-import
vp errors source export sales --open
# ... after editing the CSV
vp errors source retry sales
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