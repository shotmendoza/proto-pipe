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
    "column_type_registry",
})

NUMERIC_DUCKDB_TYPES = frozenset({
    "DOUBLE", "FLOAT", "REAL",
    "BIGINT", "INTEGER", "INT", "SMALLINT", "TINYINT", "HUGEINT",
    "DECIMAL", "NUMERIC",
})
"""DuckDB numeric column types that may conflict with mixed-type incoming data."""
