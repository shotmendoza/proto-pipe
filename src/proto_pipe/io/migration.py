"""Migration module — column type changes and schema auto-migration.

Owns:
  - _auto_migrate: add new columns to existing tables (moved from ingest.py)
  - ColumnTypeMigration: safe column type change via temp table swap

Imports only from db.py and integrity.py — no dependency on ingest.py.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import duckdb
import pandas as pd

from proto_pipe.io.db import (
    get_column_types,
    get_columns,
)
from proto_pipe.pipelines.integrity import (
    IntegrityResult,
    check_type_compatibility,
    write_integrity_flags,
    apply_declared_types
)


# ---------------------------------------------------------------------------
# _auto_migrate (moved from ingest.py)
# ---------------------------------------------------------------------------

def auto_migrate(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    df: pd.DataFrame,
) -> list[str]:
    """Add columns present in df but missing from the table.

    New columns are typed based on pandas dtype inference. Does not
    remove or rename existing columns.

    :param conn:  Open DuckDB connection.
    :param table: Target table name.
    :param df:    Incoming DataFrame.
    :return: List of column names newly added to the table.
    """
    existing = get_columns(conn, table)
    new_cols = [col for col in df.columns if col not in existing]

    for col in new_cols:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            sql_type = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "DOUBLE"
        elif pd.api.types.is_bool_dtype(dtype):
            sql_type = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_type = "TIMESTAMPTZ"
        else:
            sql_type = "VARCHAR"

        conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" {sql_type}')
        print(f"  [migrate] Added column '{col}' ({sql_type}) to '{table}'")

    return new_cols


# ---------------------------------------------------------------------------
# MigrationResult
# ---------------------------------------------------------------------------

@dataclass
class MigrationResult:
    """Result of a column type migration attempt.

    Attributes:
        success: True if migration completed without errors.
        rows_migrated:   Number of rows in the migrated table.
        rows_flagged:    Number of rows flagged in force mode.
        columns_changed: Column names whose types were changed.
        error:           Error message if migration failed, None otherwise.
    """
    success: bool
    rows_migrated: int = 0
    rows_flagged: int = 0
    columns_changed: list[str] = field(default_factory=list)
    error: str | None = None


# ---------------------------------------------------------------------------
# ColumnTypeMigration
# ---------------------------------------------------------------------------

class ColumnTypeMigration:
    """Manages safe column type changes on existing DuckDB tables.

    Pre-scans existing data with TRY_CAST before touching the table.
    Uses a temp table swap so the original is never partially modified.

    Two modes:
      execute()       — blocks if any rows fail the pre-scan
      execute_force() — flags incompatible rows to flagged_rows, migrates the rest

    :param conn:      Open DuckDB connection.
    :param table:     Target table name.
    :param new_types: {column_name: new_declared_type} — only changed columns
                      needed, but passing all columns is safe.
    :param pk_col:    Primary key column for row identity in flagged_rows, or None.
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        table: str,
        new_types: dict[str, str],
        pk_col: str | None = None,
    ) -> None:
        self._conn = conn
        self._table = table
        self._pk_col = pk_col

        # Only track columns whose type is actually changing
        existing = get_column_types(conn, table)
        self._new_types = {
            col: dtype
            for col, dtype in new_types.items()
            if col in existing and existing.get(col) != dtype.upper().split("|")[0]
        }
        self._columns_changed = list(self._new_types.keys())

    def has_changes(self) -> bool:
        """Return True if any columns actually need a type change."""
        return bool(self._columns_changed)

    def pre_scan(self) -> list[IntegrityResult]:
        """Check existing table data against the new declared types.

        Returns a list of IntegrityResult for any rows that would fail.
        Empty list means migration is safe to proceed.
        """
        if not self._columns_changed:
            return []

        existing_df = self._conn.execute(
            f'SELECT * FROM "{self._table}"'
        ).df()

        if existing_df.empty:
            return []

        return check_type_compatibility(
            df=existing_df,
            reference_types=self._new_types,
            conn=self._conn,
            pk_col=self._pk_col,
        )

    def execute(self) -> MigrationResult:
        """Migrate column types. Blocks if any rows are incompatible."""
        if not self._columns_changed:
            return MigrationResult(success=True)

        issues = self.pre_scan()
        if issues:
            return MigrationResult(
                success=False,
                error=(
                    f"{len(issues)} row(s) cannot be cast to the new type(s). "
                    f"Fix the data first, or use --force to flag and proceed."
                ),
                rows_flagged=len(issues),
            )

        return self._do_swap(df=None)

    def execute_force(self) -> MigrationResult:
        """Migrate column types, flagging incompatible rows instead of blocking."""
        if not self._columns_changed:
            return MigrationResult(success=True)

        issues = self.pre_scan()
        flagged_count = 0

        existing_df = self._conn.execute(
            f'SELECT * FROM "{self._table}"'
        ).df()

        if issues and not existing_df.empty:
            flagged_count = write_integrity_flags(self._conn, self._table, issues)

            if self._pk_col and self._pk_col in existing_df.columns:
                bad_pks = {i.pk_value for i in issues if i.pk_value is not None}
                if bad_pks:
                    existing_df = existing_df[
                        ~existing_df[self._pk_col].astype(str).isin(bad_pks)
                    ].copy()

        return self._do_swap(
            df=existing_df if issues else None,
            rows_flagged=flagged_count,
        )

    def _do_swap(
        self,
        df: pd.DataFrame | None,
        rows_flagged: int = 0,
    ) -> MigrationResult:
        """Execute the temp table swap."""
        tmp_name = f"_migration_tmp_{self._table}"
        try:
            if df is None:
                df = self._conn.execute(f'SELECT * FROM "{self._table}"').df()

            df = apply_declared_types(df, self._new_types)

            self._conn.execute(f'CREATE TABLE "{tmp_name}" AS SELECT * FROM df')
            self._conn.execute(f'DROP TABLE "{self._table}"')
            self._conn.execute(
                f'CREATE TABLE "{self._table}" AS SELECT * FROM "{tmp_name}"'
            )
            self._conn.execute(f'DROP TABLE IF EXISTS "{tmp_name}"')

            return MigrationResult(
                success=True,
                rows_migrated=len(df),
                rows_flagged=rows_flagged,
                columns_changed=self._columns_changed,
            )

        except Exception as exc:
            try:
                self._conn.execute(f'DROP TABLE IF EXISTS "{tmp_name}"')
            except Exception:
                pass
            return MigrationResult(
                success=False,
                error=f"Migration failed: {exc}",
            )
