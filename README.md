# Validation Pipeline

A config-driven data validation and reporting pipeline backed by DuckDB. Ingest CSV and Excel files, run quality checks, and produce deliverable reports — all from YAML config with no code changes required.

---

## Table of Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
  - [pipeline.yaml](#pipelineyaml)
  - [sources\_config.yaml](#sources_configyaml)
  - [reports\_config.yaml](#reports_configyaml)
  - [deliverables\_config.yaml](#deliverables_configyaml)
- [CLI Reference](#cli-reference)
- [Built-in Checks](#built-in-checks)
- [Programmatic Usage](#programmatic-usage)
- [How Data Flows](#how-data-flows)

---

## Overview

The pipeline has three stages:

1. **Ingest** — scans an incoming directory, matches files to source definitions by glob pattern, and loads them into DuckDB tables. New columns are auto-migrated; failures are logged and skipped so the run continues.
2. **Validate** — runs registered quality checks (null checks, range checks, schema checks, duplicate checks) against ingested tables. Uses watermarks so only new/modified rows are re-checked on subsequent runs.
3. **Deliver** — queries the validated tables with optional date and field filters, then writes the results as CSV files or multi-sheet Excel workbooks.

All three stages can be run individually or chained together with `run-all`.

---

## Requirements

- Python 3.11 or higher
- No external database server required — DuckDB is embedded

---

## Installation

```bash
pip install -e .
```

This installs the `validation-pipeline` CLI command and all dependencies (`duckdb`, `pandas`, `openpyxl`, `pyyaml`, `click`).

For development extras (pytest):

```bash
pip install -e ".[dev]"
```

---

## Quick Start

### 1. Scaffold config files

```bash
validation-pipeline init
```

This creates a `pipeline.yaml` in your working directory and a `config/` folder containing:
- `sources_config.yaml`
- `reports_config.yaml`
- `deliverables_config.yaml`

### 2. Edit `pipeline.yaml`

Update the paths to match your project layout — where incoming files land, where databases should live, and where output reports should be written.

### 3. Configure your sources

Edit `config/sources_config.yaml` to define file naming patterns and map them to DuckDB table names.

### 4. Configure your reports and checks

Edit `config/reports_config.yaml` to define which checks run against which tables.

### 5. Configure your deliverables

Edit `config/deliverables_config.yaml` to define which reports go into which output files, what format to use (CSV or Excel), and what date/field filters to apply.

### 6. Initialise the databases

```bash
validation-pipeline db-init
```

Creates the DuckDB files and bootstraps empty tables. Safe to re-run — existing tables and watermarks are left untouched.

### 7. Drop files and run

```bash
# Drop your CSV/Excel files into data/incoming/ (or wherever incoming_dir points)

validation-pipeline run-all --deliverable monthly_sales_pack
```

Or run each stage separately:

```bash
validation-pipeline ingest
validation-pipeline validate
validation-pipeline pull-report monthly_sales_pack
```

---

## Project Structure

```
your-project/
├── pipeline.yaml                  # Master path config (created by init)
├── config/
│   ├── sources_config.yaml        # File patterns → DuckDB tables
│   ├── reports_config.yaml        # Checks per table
│   └── deliverables_config.yaml   # Output files, sheets, filters
├── data/
│   ├── incoming/                  # Drop CSV/Excel files here
│   ├── pipeline.db                # Main DuckDB database
│   └── watermarks.db              # Watermark tracking database
└── output/
    └── reports/                   # Generated deliverables land here
```

All paths are configurable in `pipeline.yaml`.

---

## Configuration

### pipeline.yaml

The master settings file. All CLI commands read from this file, and any path can be overridden at runtime with a `--flag`.

```yaml
paths:
  sources_config:       "config/sources_config.yaml"
  reports_config:       "config/reports_config.yaml"
  deliverables_config:  "config/deliverables_config.yaml"
  pipeline_db:          "data/pipeline.db"
  watermark_db:         "data/watermarks.db"
  incoming_dir:         "data/incoming/"
  output_dir:           "output/reports/"
```

### sources\_config.yaml

Maps file glob patterns to target DuckDB table names.

```yaml
sources:
  - target_table: "sales"
    timestamp_col: "order_date"    # Required column — file is rejected if missing
    patterns:
      - "sales_*.csv"
      - "sales_*.xlsx"

  - target_table: "inventory"
    patterns:
      - "inventory_*.csv"
```

- **`target_table`** — the DuckDB table name data is loaded into
- **`timestamp_col`** — if set, files missing this column are rejected before loading
- **`patterns`** — one or more glob patterns matched against the filename

### reports\_config.yaml

Defines checks to run against each table. Supports reusable templates and inline check definitions.

```yaml
templates:
  no_nulls:
    name: null_check

  price_in_range:
    name: range_check
    params:
      col: "price"
      min_val: 0
      max_val: 100000

reports:
  - name: "daily_sales_validation"
    source:
      table: "sales"
    checks:
      - template: no_nulls
      - template: price_in_range
      - name: duplicate_check
        params:
          subset: ["order_id"]
```

### deliverables\_config.yaml

Defines output files — which reports to include, what format to produce, and what filters to apply.

```yaml
deliverables:
  - name: "monthly_sales_pack"
    format: xlsx                              # One file, multiple sheets
    filename_template: "sales_{date}.xlsx"    # {date} = YYYY-MM-DD of run
    output_dir: "output/reports/"
    reports:
      - name: "daily_sales_validation"
        sheet: "Sales"
        filters:
          date_filters:
            - col: "order_date"
              from: "2026-01-01"
              to:   "2026-03-31"
          field_filters:
            - col: "region"
              values: ["EMEA", "APAC"]

      - name: "inventory_validation"
        sheet: "Inventory"
        filters:
          date_filters:
            - col: "updated_at"
              from: "2026-01-01"

  - name: "ops_daily_drop"
    format: csv                               # One file per report
    filename_template: "{report_name}_{date}.csv"
    reports:
      - name: "daily_sales_validation"
        filters:
          date_filters:
            - col: "order_date"
              from: "2026-03-01"
```

**Formats:**
- `xlsx` — all reports written as separate sheets in a single Excel file
- `csv` — one CSV file per report; `{report_name}` is substituted in the filename

---

## CLI Reference

### `init`
Scaffold starter config files into your project.
```bash
validation-pipeline init [--output config] [--force]
```

### `db-init`
Create DuckDB files and bootstrap tables. Safe to re-run.
```bash
validation-pipeline db-init
```

### `config show`
Print all current path settings.
```bash
validation-pipeline config show
```

### `config set`
Update a path in `pipeline.yaml`.
```bash
validation-pipeline config set incoming_dir /data/drops/
```
Valid keys: `sources_config`, `reports_config`, `deliverables_config`, `pipeline_db`, `watermark_db`, `incoming_dir`, `output_dir`

### `ingest`
Scan the incoming directory and load matching files into DuckDB.
```bash
validation-pipeline ingest [--mode append|replace] [--validate]
```
- `--mode replace` drops and rebuilds the table from the file
- `--validate` runs registered checks immediately after each file loads

### `validate`
Run registered checks against ingested tables.
```bash
validation-pipeline validate [--table TABLE]
```
Uses watermarks — only new/modified rows are checked on subsequent runs.

### `update-table`
Re-ingest a specific file into a specific table.
```bash
validation-pipeline update-table sales data/drops/sales_march.csv
validation-pipeline update-table sales data/drops/sales_march.csv --mode replace
```

### `pull-report`
Query tables and write deliverable output.
```bash
validation-pipeline pull-report monthly_sales_pack
validation-pipeline pull-report monthly_sales_pack \
  --date-from 2026-01-01 --date-to 2026-03-31 --date-col order_date
```

### `run-all`
Chain ingest → validate → pull-report in one command.
```bash
validation-pipeline run-all --deliverable monthly_sales_pack
validation-pipeline run-all --deliverable monthly_sales_pack --ignore-flagged
```
Stops before producing deliverables if flagged rows exist, unless `--ignore-flagged` is passed.

### `checks`
List all available built-in checks and their parameters.
```bash
validation-pipeline checks
```

---

## Built-in Checks

| Check | Parameters | Description |
|---|---|---|
| `null_check` | none | Checks all columns for null values |
| `range_check` | `col`, `min_val`, `max_val` | Checks a column's values fall within a numeric range |
| `schema_check` | `expected_cols` (list) | Checks the table has exactly the expected columns |
| `duplicate_check` | `subset` (list, optional) | Checks for duplicate rows, optionally scoped to a column subset |

---

## Programmatic Usage

For embedding the pipeline in a larger Python workflow or scheduler, use `main_v2.py` as a starting point, or import directly:

```python
from validation_pipeline import (
    load_config, register_from_config,
    check_registry, report_registry,
    WatermarkStore,
    ingest_directory,
    run_all_reports,
    produce_deliverable,
    query_table,
)

settings = load_settings()
paths = settings["paths"]

# Ingest
sources_config = load_config(paths["sources_config"])
ingest_directory(paths["incoming_dir"], sources_config["sources"], paths["pipeline_db"])

# Validate
reports_config = load_config(paths["reports_config"])
register_from_config(reports_config, check_registry, report_registry)
watermark_store = WatermarkStore(paths["watermark_db"])
results = run_all_reports(report_registry, check_registry, watermark_store)

# Deliver
import duckdb
conn = duckdb.connect(paths["pipeline_db"])
df = query_table(conn, "sales", filters={"date_filters": [{"col": "order_date", "from": "2026-01-01"}]})
```

---

## How Data Flows

```
data/incoming/
    sales_march.csv
    inventory_2026.xlsx
         │
         ▼  ingest
    pipeline.db
    ├── sales          (DuckDB table)
    ├── inventory      (DuckDB table)
    ├── ingest_log     (load history)
    ├── flagged_rows   (rows needing review)
    └── report_runs    (deliverable history)
         │
         ▼  validate (watermark-filtered)
    checks run → results logged
    watermarks updated
         │
         ▼  pull-report
    output/reports/
        sales_2026-03-26.xlsx   (Sales sheet + Inventory sheet)
        daily_sales_2026-03-26.csv
```

Failures at any stage are logged and skipped — a bad file doesn't stop the rest of the ingest run, and a failing check doesn't stop other reports from running.
