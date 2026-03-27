# validation-pipeline

A config-driven data validation and reporting pipeline backed by DuckDB.
Ingest CSV and Excel files, run checks, and produce deliverable reports —
all driven by YAML with no code changes required for new sources or reports.

---

## Installation

```bash
pip install validation-pipeline
```

---

## Quickstart

```bash
# 1. Scaffold config files into your project
vp init

# 2. Edit your configs
#    config/sources_config.yaml        — file naming conventions → DuckDB tables
#    config/reports_config.yaml        — checks and validation rules
#    config/deliverables_config.yaml   — output format and filters
#    config/views_config.yaml          — shared transformation views (optional)
#    pipeline.yaml                     — path defaults

# 3. Create DuckDB databases, bootstrap tables, and create views
vp db-init

# 4. Drop files into data/incoming/ then run
vp ingest
vp validate
vp pull-report <deliverable-name>

# Or chain everything in one command
vp run-all --deliverable <deliverable-name>
```

---

## CLI Reference

| Command | Description |
|---|---|
| `init` | Scaffold config files into your project |
| `db-init` | Create DuckDB files, bootstrap tables, and create views |
| `config show` | Print current path settings from pipeline.yaml |
| `config set <key> <value>` | Update a path setting |
| `ingest` | Load files from incoming dir into DuckDB. Already-ingested files are skipped automatically. |
| `ingest --validate` | Load files and run checks immediately after |
| `validate` | Run registered checks against ingested tables |
| `update-table <table> <file>` | Re-ingest a specific file into a specific table |
| `pull-report <name>` | Query tables with filters and write deliverable output |
| `run-all` | Chain ingest → validate → refresh-views → pull-report |
| `checks` | List all available built-in checks |
| `refresh-views` | Drop and recreate all views from views_config.yaml |
| `export-flagged --table <name>` | Export flagged rows to CSV for manual correction |
| `import-corrections <file> --table <name>` | Apply a corrected CSV back to the source table |

---

## Project Structure

```
your-project/
├── pipeline.yaml                     # path defaults (created by init)
├── config/
│   ├── sources_config.yaml           # file naming conventions per table
│   ├── reports_config.yaml           # checks, templates, reports
│   ├── deliverables_config.yaml      # output format and filters per deliverable
│   ├── views_config.yaml             # shared transformation views
│   └── sql/
│       ├── views/                    # SQL files for shared views
│       └── *.sql                     # SQL files for deliverable queries
├── data/
│   ├── incoming/                     # drop CSV/Excel files here
│   ├── pipeline.db                   # DuckDB — ingested tables + logs
│   └── watermarks.db                 # DuckDB — watermark tracking
└── output/
    └── reports/                      # deliverable output files
```

---

## Package Structure

```
├── README.md
├── config
│       ├── deliverables_config.yaml
│       ├── pipeline.yaml
│       ├── reports_config.yaml
│       ├── sources_config.yaml
│       └── views_config.yaml
├── docs
│       ├── adding_checks.md
│       ├── adding_deliverables.md
│       ├── adding_reports.md
│       ├── first_time_setup.md
│       └── reviewing_flagged_rows.md
├── pyproject.toml
├── src
│        ├── __init__.py
│        ├── __pycache__
│        ├── checks
│        │       ├── __init__.py
│        │       ├── __pycache__
│        │       ├── built_in.py
│        │       ├── helpers.py
│        │       └── runner.py
│        ├── cli.py
│        ├── io
│        │       ├── __init__.py
│        │       ├── __pycache__
│        │       ├── data.py
│        │       ├── ingest.py
│        │       ├── registry.py
│        │       └── settings.py
│        ├── main.py
│        ├── pipelines
│        │       ├── __init__.py
│        │       ├── __pycache__
│        │       └── watermark.py
│        ├── registry
│        │       ├── __init__.py
│        │       ├── __pycache__
│        │       └── base.py
│        └── reports
│            ├── __init__.py
│            ├── __pycache__
│            ├── corrections.py
│            ├── query.py
│            ├── runner.py
│            └── views.py
├── tests
│        ├── __init__.py
│        ├── __pycache__
│        │       ├── __init__.cpython-313.pyc
│        │       ├── conftest.cpython-313-pytest-9.0.2.pyc
│        │       ├── test_checks.cpython-313-pytest-9.0.2.pyc
│        │       ├── test_checks.cpython-313.pyc
│        │       ├── test_custom_checks.cpython-313-pytest-9.0.2.pyc
│        │       ├── test_custom_checks.cpython-313.pyc
│        │       ├── test_deliverables.cpython-313-pytest-9.0.2.pyc
│        │       ├── test_deliverables.cpython-313.pyc
│        │       ├── test_ingest.cpython-313-pytest-9.0.2.pyc
│        │       ├── test_ingest.cpython-313.pyc
│        │       ├── test_ingest_chunking.cpython-313-pytest-9.0.2.pyc
│        │       ├── test_ingest_corrections.cpython-313-pytest-9.0.2.pyc
│        │       ├── test_ingest_corrections_runner.cpython-313-pytest-9.0.2.pyc
│        │       ├── test_query.cpython-313-pytest-9.0.2.pyc
│        │       ├── test_query.cpython-313.pyc
│        │       ├── test_runner.cpython-313-pytest-9.0.2.pyc
│        │       ├── test_runner.cpython-313.pyc
│        │       ├── test_views_corrections_ingest.cpython-313-pytest-9.0.2.pyc
│        │       ├── test_watermark.cpython-313-pytest-9.0.2.pyc
│        │       ├── test_watermark.cpython-313.pyc
│        │       ├── test_workflow.cpython-313-pytest-9.0.2.pyc
│        │       └── test_workflow.cpython-313.pyc
│        ├── conftest.py
│        ├── test_checks.py
│        ├── test_custom_checks.py
│        ├── test_deliverables.py
│        ├── test_ingest.py
│        ├── test_ingest_corrections_runner.py
│        ├── test_query.py
│        ├── test_runner.py
│        ├── test_views_corrections_ingest.py
│        ├── test_watermark.py
│        └── test_workflow.py
```

---

## DuckDB Tables

The pipeline creates and manages these tables automatically:

| Table | DB | Description |
|---|---|---|
| `<source_table>` | pipeline.db | One table per source defined in sources_config.yaml |
| `ingest_log` | pipeline.db | Record of every file ingested, including failures and skips |
| `flagged_rows` | pipeline.db | Rows flagged by checks for manual review and correction |
| `report_runs` | pipeline.db | Record of every deliverable produced |
| `watermarks` | watermarks.db | Last processed timestamp per report |

---

## Adding a New Source

1. Add a pattern entry to `config/sources_config.yaml`:

```yaml
sources:
  - name: "orders"
    patterns:
      - "orders_*.csv"
      - "Orders_*.xlsx"
    target_table: "orders"
    timestamp_col: "updated_at"
    primary_key: "order_id"     # used by import-corrections to match corrected rows
```

2. Drop a matching file into `data/incoming/` and run `vp ingest`.
   The table is created automatically on first ingest — no `db-init` needed again.
   Files already ingested are skipped on subsequent runs.

---

## Adding a New Report

Add an entry to `config/reports_config.yaml`. No code changes required.
See `docs/adding_reports.md` for full key reference.

---

## Adding a New Deliverable

Add an entry to `config/deliverables_config.yaml`. Deliverables support two
query paths per report: a `sql_file` for custom joins and transformations, or
`filters` for simple single-table queries with dynamic date tokens.
See `docs/adding_deliverables.md` for full key reference.

---

## Adding a Custom Check

```python
from validation_pipeline import check_registry
from validation_pipeline.checks.registry_helpers import register_custom_check

def check_margin(context, col="margin", threshold=0.2):
    df = context["df"]
    below = df[df[col] < threshold]
    if not below.empty:
        raise ValueError(f"{len(below)} rows below margin threshold {threshold}")
    return {"violations": 0, "threshold": threshold}

register_custom_check("margin_check", check_margin, check_registry)
```

Call `register_custom_check` before loading your config.
See `docs/adding_checks.md` for full details.

---

## Shared Transformation Views

Define reusable SQL transformations in `config/views_config.yaml` and reference
them by name in any deliverable SQL file. Useful when multiple carriers or
deliverables need the same column standardisation or join logic.

```yaml
# config/views_config.yaml
views:
  - name: "clean_sales"
    sql_file: "config/sql/views/clean_sales.sql"
```

```sql
-- config/sql/carrier_a_sales.sql
SELECT order_id, price, region
FROM clean_sales          -- shared view, transformation already applied
WHERE region = 'EMEA'
```

Views are created by `vp db-init` and refreshed by `vp refresh-views`.
`vp run-all` refreshes them automatically before producing deliverables.

---

## Correcting Flagged Rows

When validation flags bad data, export the flagged rows, fix them, and import
the corrections back — the fix is applied directly to the DuckDB table.

```bash
# Export flagged rows for a table to CSV
vp export-flagged --table sales

# Edit the CSV to fix the values, then re-import
vp import-corrections output/reports/flagged_sales.csv --table sales
```

The corrected rows are updated in the table and cleared from `flagged_rows`.
The primary key used for matching is defined in `sources_config.yaml` under
`primary_key`, or overridden at runtime with `--key`.

---

## Path Configuration

All paths default to sensible locations relative to your project root.
Override any path permanently via `pipeline.yaml` or temporarily via CLI flags:

```bash
vp config set incoming_dir /data/drops/
vp config set pipeline_db /mnt/shared/pipeline.db
vp config show
```

| Key | Default |
|---|---|
| `sources_config` | `config/sources_config.yaml` |
| `reports_config` | `config/reports_config.yaml` |
| `deliverables_config` | `config/deliverables_config.yaml` |
| `views_config` | `config/views_config.yaml` |
| `pipeline_db` | `data/pipeline.db` |
| `watermark_db` | `data/watermarks.db` |
| `incoming_dir` | `data/incoming/` |
| `output_dir` | `output/reports/` |
