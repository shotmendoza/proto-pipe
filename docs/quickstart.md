# Quickstart

This covers the things you'll do regularly once the pipeline is set up.
For first-time installation and config, see `docs/first_time_setup.md`.

---

## The three flows

```
1. Run everything        vp run-all --deliverable <n>
2. Fix ingest conflicts  vp errors source export <n> --open → edit → vp errors source retry <n>
3. Fix validation flags  vp errors report export <n> → fix at source → re-ingest → re-validate
```

---

## 1. Daily / monthly run

```bash
vp run-all --deliverable monthly_pack
```

Runs: ingest → validate → refresh views → produce deliverable.

**If it stops with an ingest conflict warning:**
Rows arrived whose values differ from what's already in the table.
These block the deliverable until resolved.

```bash
vp errors source sales              # browse conflicts
vp errors source export sales --open # export + open in default app for editing
# edit the file — fix the bad values, save
vp errors source retry sales         # apply corrections through ingest cycle
vp run-all --deliverable monthly_pack
```

To produce the deliverable without resolving:

```bash
vp run-all --deliverable monthly_pack --ignore-flagged
```

**If it completes with a validation warning:**
Rows failed a business logic check. The deliverable was still produced.
Validation flags are warnings — fix at source, re-ingest, re-validate.

```bash
vp errors report                     # browse check failures
vp errors report export sales_validation  # export Detail + Summary sheets to Excel
# fix the values in the source file, then:
vp ingest
vp validate
```

---

## 2. Loading new files

Drop files into `incoming_dir` and run:

```bash
vp ingest
```

Files already ingested are skipped automatically. To see ingest history:

```bash
vp view table ingest_state
```

---

## 3. Adding a new source, report, or deliverable

Use the interactive wizards — no YAML editing required:

```bash
vp new source        # define a new data source
vp new report        # define checks and column mappings
vp new deliverable   # define output format and recipients
```

Or edit configs manually. **New source** (`config/sources_config.yaml`):

```yaml
sources:
  - name: "orders"
    patterns: ["orders_*.csv"]
    target_table: "orders"
    timestamp_col: "updated_at"
    primary_key: "order_id"
    on_duplicate: flag
```

**New report** (`config/reports_config.yaml`):

```yaml
reports:
  - name: "orders_validation"
    source:
      type: duckdb
      path: "data/pipeline.db"
      table: "orders"
    alias_map:
      - param: col
        column: total
    checks:
      - name: null_check
      - name: range_check
        params:
          min_val: 0
          max_val: 100000
```

**New deliverable** (`config/deliverables_config.yaml`):

```yaml
deliverables:
  - name: "orders_pack"
    format: xlsx
    filename_template: "orders_{date}.xlsx"
    reports:
      - name: "orders_validation"
        sheet: "Orders"
        filters:
          date_filters:
            - col: "updated_at"
              to: "end_of_last_month"
```

---

## 4. Writing a custom check

Checks return `pd.Series[bool]` — `True` where a row passes, `False` where
it fails. Use the `@custom_check` decorator and point the pipeline at your
module in `pipeline.yaml`.

```python
# my_checks.py
import pandas as pd
from proto_pipe.checks.helpers import custom_check


@custom_check("no_negatives", kind="check")
def check_no_negatives(col: pd.Series) -> pd.Series[bool]:
    """Check that values in a column are not negative."""
    return col >= 0


@custom_check("margin_check", kind="check")
def check_margin(col: pd.Series, threshold: float = 0.2) -> pd.Series[bool]:
    """Check that margin is above the declared threshold."""
    return col >= threshold
```

Register the module in `pipeline.yaml`:

```yaml
custom_checks_module: "my_checks.py"
```

Run `vp funcs` to confirm checks registered correctly. Checks with `str` or
`pd.Series` column params appear as column selectors in `vp new report`.
Scalar params (`float`, `int`) are prompted as free text and broadcast across
all alias_map column runs.

**Transforms** — use `kind="transform"` to modify column values. Transforms
run after all checks and write back to the table.

```python
@custom_check("normalize_region", kind="transform")
def normalize_region(col: pd.Series) -> pd.Series:
    """Uppercase and strip the region column."""
    return col.str.upper().str.strip()
```

See `docs/adding_checks.md` for the full reference.

---

## 5. Adding a SQL file for a deliverable

Use `vp new sql` to scaffold one interactively, or write it manually.

```sql
-- config/sql/carrier_a_sales.sql
SELECT
    s.order_id,
    s.gross_premium,
    c.customer_name,
    c.region
FROM sales s
JOIN customers c ON s.customer_id = c.customer_id
WHERE s.order_date <= (date_trunc('month', current_date) - INTERVAL '1 day')
  AND c.region = 'EMEA'
```

Reference the file in `deliverables_config.yaml`:

```yaml
deliverables:
  - name: "carrier_a_pack"
    format: xlsx
    filename_template: "carrier_a_{date}.xlsx"
    reports:
      - name: "carrier_a_sales"
        sheet: "Sales"
        sql_file: "config/sql/carrier_a_sales.sql"
```

**Shared views** — define reusable transformations in `views_config.yaml`
and reference them by name in any SQL file. Run `vp refresh views` after
editing a view SQL file.

**Macros** — define reusable column-level expressions with `vp new macro`
and call them in any SQL file. Run `vp init db` after adding a macro to
re-register it.

See `docs/adding_deliverables.md` for the full reference.

---

## Quick reference

**Setup & config**

| Task                              | Command                                                     |
|-----------------------------------|-------------------------------------------------------------|
| Workflow guide                    | `vp help`                                                   |
| Check path settings               | `vp config show`                                            |
| Update a path                     | `vp config set <key> <value>`                               |
| Inspect check functions           | `vp funcs`                                                  |
| New source / report / deliverable | `vp new source` / `vp new report` / `vp new deliverable`    |
| New SQL file / view / macro       | `vp new sql` / `vp new view` / `vp new macro`               |
| Edit existing config              | `vp edit source` / `vp edit report` / `vp edit deliverable` |

**Ingest**

| Task                         | Command                              |
|------------------------------|--------------------------------------|
| Load source files            | `vp ingest`                          |
| View ingest history          | `vp view table ingest_state`         |
| Browse ingest conflicts      | `vp errors source <n>`               |
| Export conflicts for editing | `vp errors source export <n> --open` |
| Apply corrections            | `vp errors source retry <n>`         |
| Clear conflicts              | `vp errors source clear <n>`         |

**Validation**

| Task                  | Command                       |
|-----------------------|-------------------------------|
| Run checks            | `vp validate`                 |
| Browse check failures | `vp errors report`            |
| Export check failures | `vp errors report export <n>` |

**Deliverables**

| Task                       | Command                                         |
|----------------------------|-------------------------------------------------|
| Produce a deliverable      | `vp deliver <n>`                                |
| Preview deliverable output | `vp view deliverable <n>`                       |
| Refresh views              | `vp refresh views`                              |
| Run everything             | `vp run-all --deliverable <n>`                  |
| Skip conflict block        | `vp run-all --deliverable <n> --ignore-flagged` |
| Check pipeline health      | `vp status`                                     |
