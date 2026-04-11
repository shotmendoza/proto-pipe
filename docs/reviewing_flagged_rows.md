# Reviewing Flagged Rows

The pipeline maintains two separate flag tables with different meanings and
different correction paths. Understanding which one you're looking at matters.

---

## Two flag tables — different meanings

| Table              | Written by    | Meaning                                                                      | Blocks deliverables? |
|--------------------|---------------|------------------------------------------------------------------------------|----------------------|
| `source_block`     | `vp ingest`   | Ingest-time conflicts — duplicate key with changed content, or type mismatch | Yes                  |
| `validation_block` | `vp validate` | Business logic check failures                                                | No — warns only      |

`vp errors source` and its subcommands operate on `source_block`.
`vp errors report` operates on `validation_block`.

---

## Ingest conflicts (source_block)

Rows land in `source_block` during ingest when:

- A file contains a row whose primary key already exists but whose content changed
  (`on_duplicate: flag` is set on the source)
- A row has values that can't be cast to the column's declared type

The incoming row is skipped. The original row stays in the table until you resolve
the conflict.

### View ingest flags

```bash
vp errors source                    # all tables
vp errors source sales              # one table
```

For an enriched view that joins flag metadata to the actual source row so you
can see the bad data in context:

```bash
vp errors source edit sales
```

---

## Correction workflow

### Step 1 — Open the flagged rows for editing

```bash
vp errors source export sales --open
```

This exports the enriched flagged rows (source data + flag context) to
`incoming_dir` as `<pattern>_flagged_<date>.csv` (e.g. `sales_flagged_2026-04-01.csv`)
and opens it in your default application. The file is named to match your source
pattern so `vp ingest` can pick it up naturally.

Edit the data columns that are wrong and save the file. Leave any `_` prefixed
columns (`_flag_reason`, `_flag_columns`, `_flag_check`) as-is — they are stripped
automatically before ingest.

### Step 2 — Apply corrections

```bash
vp errors source retry sales
```

This picks up the most recently modified flagged export from `incoming_dir`,
runs it through the full ingest cycle with `on_duplicate=upsert` (corrections
always overwrite), and clears resolved flags from `source_block`.

```
Applying corrections from: sales_flagged_2026-04-01.csv
[ok] 3 row(s) applied to 'sales'
[ok] 3 flag(s) cleared from source_block
```

The correction is logged in `ingest_state` with `status='correction'` so you
have an audit trail separate from normal ingests.

### Step 3 — Re-validate

```bash
vp validate --table sales
```

Confirms the corrected rows now pass checks before producing the deliverable.

---

## Other flag handling options

**Clear flags without correcting** — when you've reviewed and the data is
acceptable as-is:

```bash
vp errors source clear sales
vp errors source clear sales --check duplicate_conflict   # one check only
vp errors source clear sales --yes                        # skip confirmation
```

**Produce deliverable anyway** — flags remain but don't block output:

```bash
vp run-all --deliverable monthly_pack --ignore-flagged
```

---

## Validation flags (validation_block)

Check failures from `vp validate` go to `validation_block`. These warn but
do not block deliverables. The correction path is different — fix at source,
re-ingest, re-validate.

```bash
vp errors report                          # all reports
vp errors report sales_validation         # one report
vp errors report export sales_validation  # export detail + summary to Excel
vp errors report export sales_validation --open  # export and open for editing
```

---

## Common scenarios

**"The same row keeps getting flagged after every ingest."**
The source system is consistently sending changed values. Decide which version
is correct, apply corrections once, then consider switching `on_duplicate` to
`upsert` for that source in `sources_config.yaml` if you always want the
latest value to win.

**"I corrected the rows but they got flagged again."**
A new file arrived with the original bad values. Fix it at the source, or
switch to `on_duplicate: upsert` if the source always sends the authoritative
version.

**"I want to clear everything and start fresh."**

```bash
vp errors source clear sales --yes
```

**"I don't have a primary key column."**
Without a primary key, conflict detection and targeted corrections are not
available. `on_duplicate` falls back to `append`. Raise this with whoever
provides the source files.

**"How do I see what on_duplicate is set to?"**
Open `config/sources_config.yaml` and look for `on_duplicate` under the
relevant source. Run `vp view source <table>` to see the ingested data with
flag status alongside.
