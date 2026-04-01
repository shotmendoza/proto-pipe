---
title: proto-pipe
---

# proto-pipe

A config-driven data validation and reporting pipeline backed by DuckDB.
Ingest CSV and Excel files, run checks, and produce deliverable reports —
all driven by YAML with no code changes required for new sources or reports.

---

## Documentation

- [First Time Setup](first_time_setup) — install, scaffold, and run your first pipeline
- [Adding Checks](adding_checks) — write custom check and transform functions
- [Adding Reports](adding_reports) — define validation checks for your data
- [Adding Deliverables](adding_deliverables) — configure output files and carriers
- [Reviewing Flagged Rows](reviewing_flagged_rows) — investigate and correct flagged data

---

## Quick reference

### Day-to-day

| Step | Task                  | Command                           |
|------|-----------------------|-----------------------------------|
| 1    | Load new files        | `vp ingest`                       |
| 2    | Run checks            | `vp validate`                     |
| 3    | Produce a deliverable | `vp pull-report <name>`           |
| —    | Or run everything     | `vp run-all --deliverable <name>` |

### First-time setup

| Step | Task                     | Command              |
|------|--------------------------|----------------------|
| 1    | Scaffold config files    | `vp init`            |
| 2    | initialize database      | `vp db-init`         |
| 3    | Define a data source     | `vp new source`      |
| 4    | Define validation checks | `vp new report`      |
| 5    | Define output            | `vp new deliverable` |

### Scaffold

| Task                     | Command              |
|--------------------------|----------------------|
| New data source          | `vp new source`      |
| New report               | `vp new report`      |
| New deliverable          | `vp new deliverable` |
| New SQL view             | `vp new view`        |
| New SQL macro            | `vp new macro`       |
| New deliverable SQL file | `vp new sql`         |

### Review & fix

| Task                       | Command                        |
|----------------------------|--------------------------------|
| Browse ingest conflicts    | `vp flagged --table <n>`       |
| Open conflicts for editing | `vp flagged open <table>`      |
| Apply corrections          | `vp flagged retry <table>`     |
| Clear flags                | `vp flagged clear --table <n>` |
| Browse check failures      | `vp validated`                 |
| Export check failures      | `vp export-validation`         |

### Explore & manage

| Task                    | Command                       |
|-------------------------|-------------------------------|
| View source table       | `vp view source <table>`      |
| View deliverable output | `vp view deliverable <name>`  |
| Browse any table        | `vp table <n>`                |
| Inspect check functions | `vp funcs`                    |
| Check path settings     | `vp config show`              |
| Update a path           | `vp config set <key> <value>` |
| Workflow guide          | `vp help`                     |
