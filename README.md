# ProtoPipe

A config-driven data validation and reporting pipeline backed by DuckDB.
Ingest CSV and Excel files, run checks, and produce deliverable reports —
all driven by YAML with no code changes required for new sources or reports.

---

## How it works

```
Source files  →  Ingest  →  Report tables  →  Validate  →  Deliverables
(CSV / Excel)    (DuckDB)   (per source)       (checks)     (CSV / Excel)
```

**Sources** are raw files dropped into a directory. **Reports** are the
validated versions of those tables — checks run against them and any failing
rows are flagged. **Deliverables** are formatted outputs combining one or
more reports into a file for stakeholders.

---

## Installation

```bash
pip install protp-pipe

# Or with uv
uv add protp-pipe

# From a GitHub repository
pip install git+https://github.com/shotmendoza/protp-pipe.git
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

# 3. Create DuckDB databases, bootstrap tables
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
| `db-init` | Create DuckDB files, bootstrap tables |
| `config show` | Print current path settings from pipeline.yaml |
| `config set <key> <value>` | Update a path setting |
| `ingest` | Load source files from incoming dir into DuckDB. Already-ingested files are skipped. |
| `ingest --validate` | Load files and run checks immediately after |
| `ingest-log` | Show recent ingest attempts. Use `--status failed` to see failures. |
| `validate` | Run registered checks against ingested tables. Flags written to `validation_flags`. |
| `update-table <table> <file>` | Re-ingest a specific file into a specific table |
| `pull-report <name>` | Query tables and write deliverable output |
| `run-all` | Chain ingest → validate → refresh-views → pull-report |
| `checks` | List all available built-in checks |
| `refresh-views` | Drop and recreate all views from views_config.yaml |
| `flagged-summary` | Count open ingest conflicts by table and reason |
| `flagged-list --table <n>` | Print ingest conflict rows inline in the terminal |
| `flagged-clear --table <n>` | Clear ingest conflicts without correcting |
| `export-flagged --table <n>` | Export ingest conflict rows to CSV for correction |
| `import-corrections <file> --table <n>` | Apply a corrected CSV back to the source table |
| `check-null-overwrites --table <n>` | Manually scan a table for duplicate conflicts |
| `export-validation` | Export validation flags to a two-sheet Excel file (Detail + Summary) |

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
├── config/
│   ├── deliverables_config.yaml
│   ├── pipeline.yaml
│   ├── reports_config.yaml
│   ├── sources_config.yaml
│   └── views_config.yaml
├── docs/
│   ├── adding_checks.md
│   ├── adding_deliverables.md
│   ├── adding_reports.md
│   ├── first_time_setup.md
│   └── reviewing_flagged_rows.md
├── pyproject.toml
└── src/
    ├── __init__.py
    ├── checks/
    │   ├── built_in.py
    │   ├── helpers.py
    │   └── runner.py
    ├── cli/
    │   ├── __init__.py
    │   ├── data.py
    │   ├── flagged.py
    │   ├── reports.py
    │   ├── setup.py
    │   └── validation.py
    ├── io/
    │   ├── data.py
    │   ├── ingest.py
    │   ├── registry.py
    │   └── settings.py
    ├── pipelines/
    │   └── watermark.py
    ├── registry/
    │   └── base.py
    └── reports/
        ├── corrections.py
        ├── query.py
        ├── runner.py
        ├── validation_flags.py
        └── views.py
```

---

## DuckDB Tables

The pipeline creates and manages these tables automatically:

| Table | DB | Description |
|---|---|---|
| `<source_table>` | pipeline.db | One table per source defined in sources_config.yaml |
| `ingest_log` | pipeline.db | Record of every file ingested, including failures and skips |
| `flagged_rows` | pipeline.db | Ingest conflicts — rows that arrived with changed values for an existing record. Blocks deliverables. |
| `validation_flags` | pipeline.db | Validation failures — rows that failed a check. Warning only, does not block deliverables. |
| `report_runs` | pipeline.db | Record of every deliverable produced |
| `watermarks` | watermarks.db | Last processed timestamp per report |

---

## Flagging: two separate tables

The pipeline separates ingest problems from validation problems:

**Ingest conflicts** (`flagged_rows`) — a row arrived whose primary key
already exists but whose content changed. The incoming row is held back until
you decide whether to accept or discard it. Open ingest conflicts block
deliverable production.

**Validation flags** (`validation_flags`) — a check found bad data in an
already-ingested row. The row is in the report table but fails a quality
rule. These are warnings — the deliverable is still produced, but the export
shows which records need fixing at the source.

---

## Adding a New Source

Add a pattern entry to `config/sources_config.yaml`:

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

Drop a matching file into `data/incoming/` and run `vp ingest`. The table is
created automatically on first ingest. Files already ingested are skipped on
subsequent runs.

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
from validation_pipeline import custom_check

@custom_check("margin_check")
def check_margin(context, col="margin", threshold=0.2):
    df = context["df"]
    below = df[df[col] < threshold]
    if not below.empty:
        raise ValueError(f"{len(below)} rows below margin threshold {threshold}")
    return {"violations": 0, "threshold": threshold}
```

Point the pipeline at your module in `pipeline.yaml`:

```yaml
custom_checks_module: "my_checks.py"
```

For row-level flags, return a boolean mask alongside your result:

```python
@custom_check("no_negatives")
def check_no_negatives(context, col="price"):
    df = context["df"]
    return {"mask": df[col] < 0}   # one flag per failing row
```

See `docs/adding_checks.md` for full details and registration options.

---

## Shared Transformation Views

Define reusable SQL transformations in `config/views_config.yaml` and
reference them by name in any deliverable SQL file.

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

## Correcting Ingest Conflicts

When ingest flags conflicting rows, export them, fix the values, and import
the corrections back:

```bash
vp export-flagged --table sales
# edit the CSV
vp import-corrections output/reports/flagged_sales.csv --table sales
```

The corrected rows are updated in the table and cleared from `flagged_rows`.
The primary key for matching is defined in `sources_config.yaml` under
`primary_key`, or overridden at runtime with `--key`.

## Reviewing Validation Flags

When validation flags bad records, export the report and fix the data at source:

```bash
vp export-validation                # Detail + Summary sheets
# fix data at source, re-ingest, re-validate
```

See `docs/reviewing_flagged_rows.md` for both workflows in full.

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
