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
- [Adding Reports & Checks](adding_checks) — define validation checks for your data
- [Adding Deliverables](adding_deliverables) — configure output files and carriers
- [Reviewing Flagged Rows](reviewing_flagged_rows) — investigate and correct flagged data

---

## Quick reference

### Setting Up a Pipeline
> This is how you quickly get started with proto-pipe and get a pipeline up and running.

| Step | Task                  | Command                     |
|------|-----------------------|-----------------------------|
| 1    | First-time setup      | `vp init` then `vp db-init` |
| 2    | Load new files        | `vp ingest`                 |
| 3    | Run checks            | `vp validate`               |
| 4    | Produce a deliverable | `vp pull-report <name>`     |
| 5    | Review flagged rows   | `vp review-flagged-rows`    |

### Setting up configs, checks, and infrastructure

| Step                                 | Task                       | Command               |
|--------------------------------------|----------------------------|-----------------------|
| Raw data from folder                 | Scaffold a new source      | `vp new-source`       |
| Name and identify a report           | Scaffold a new report      | `vp new-report`       |
| Create final report for stakeholders | Scaffold a new deliverable | `vp new-deliverable`  |
| Create reusable sql queries          | Scaffold a macro           | `vp new-macro <name>` |
| Create a new SQL file                | Scaffold a new SQL file    | `vp new-sql <name>`   |

### Troubleshooting
| Task                 | Command                           |
|----------------------|-----------------------------------|
| Run everything       | `vp run-all --deliverable <name>` |
| Reset a report table | `vp table-reset`                  |
| Check current paths  | `vp config show`                  |
| Update a path        | `vp config set <key> <value>`     |
