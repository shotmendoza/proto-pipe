"""Shared constants used across proto_pipe modules."""

# Tables created and managed by the pipeline itself.
# Excluded from user-facing table lists (e.g. vp new-report, vp table).
PIPELINE_TABLES: frozenset[str] = frozenset({
    "flagged_rows",
    "ingest_log",
    "report_runs",
    "validation_flags",
    "check_params_history",
    "check_registry_metadata",
})
