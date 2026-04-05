"""Shared constants used across proto_pipe modules."""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def _find_settings_path() -> Path:
    """Walk up from CWD looking for pipeline.yaml, like git finds .git."""
    current = Path.cwd()
    for parent in [current, *current.parents]:
        candidate = parent / "pipeline.yaml"
        if candidate.exists():
            return candidate
    return Path("pipeline.yaml")  # fallback — will trigger the defaults path


# Tables created and managed by the pipeline itself.
# Excluded from user-facing table lists (e.g. vp new-report, vp table).
PIPELINE_TABLES: frozenset[str] = frozenset({
    # State tables — record-level tracking
    "source_pass",
    "validation_pass",
    # Block tables — problem records
    "source_block",
    "validation_block",
    # Ingest history
    "ingest_state",
    # Registry tables
    "check_registry_metadata",
    "column_type_registry",
    # Legacy names — kept during migration window so vp db-init --migrate
    # can detect and rename them. Remove once all DBs are migrated.
    "flagged_rows",
    "ingest_log",
    "report_runs",
    "validation_flags",
    "check_params_history",
})

NUMERIC_DUCKDB_TYPES = frozenset({
    "DOUBLE", "FLOAT", "REAL",
    "BIGINT", "INTEGER", "INT", "SMALLINT", "TINYINT", "HUGEINT",
    "DECIMAL", "NUMERIC",
})
"""DuckDB numeric column types that may conflict with mixed-type incoming data."""


DATE_FORMATS = [
    ("%Y-%m-%d",  "2026-01-06"),
    ("%m/%d/%Y",  "01/06/2026"),
    ("%m-%d-%y",  "01-06-26"),
    ("%d/%m/%Y",  "06/01/2026"),
    ("%Y/%m/%d",  "2026/01/06"),
    ("%m/%d/%y",  "01/06/26"),
]

# Common DuckDB types shown in all type selection prompts
DUCKDB_TYPES = ["VARCHAR", "DOUBLE", "BIGINT", "BOOLEAN", "DATE", "TIMESTAMPTZ"]
_DEFAULTS = {
    "paths": {
        "sources_config": "config/sources_config.yaml",
        "reports_config": "config/reports_config.yaml",
        "deliverables_config": "config/deliverables_config.yaml",
        "views_config": "config/views_config.yaml",
        "pipeline_db": "data/pipeline.db",
        "watermark_db": "data/watermarks.db",
        "incoming_dir": "data/incoming/",
        "output_dir": "output/reports/",
        "sql_dir": "sql/",
    },
    "multi_select_params": True,
    "macros_dir": "macros",
}
VALID_PATH_KEYS = list(_DEFAULTS["paths"].keys())

NULLABLE_EXTENSION_DTYPES = (
    pd.Int8Dtype,
    pd.Int16Dtype,
    pd.Int32Dtype,
    pd.Int64Dtype,
    pd.UInt8Dtype,
    pd.UInt16Dtype,
    pd.UInt32Dtype,
    pd.UInt64Dtype,
    pd.BooleanDtype,
)
