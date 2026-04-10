"""Migration module — column type changes and schema auto-migration.

Owns:
  - auto_migrate: add new columns to existing tables
  - apply_declared_types: cast DataFrame columns to declared types
  - check_type_compatibility: TRY_CAST pre-scan for type compatibility
  - IntegrityResult: dataclass for pre-scan results
  - ColumnTypeMigration: safe column type change via temp table swap
  - migrate_pipeline_schema: rename/create pipeline tables for vp db-init --migrate

Dependency chain:
  db.py ← no proto_pipe imports
  flagging.py ← db.py only
  migration.py ← db.py + flagging.py
  ingest.py ← db.py + flagging.py + migration.py
"""
from __future__ import annotations

from dataclasses import dataclass, field

import duckdb
import pandas as pd

from proto_pipe.io.db import (
    get_column_types,
    get_columns,
    flag_id_for,
)


# ---------------------------------------------------------------------------
# IntegrityResult
# ---------------------------------------------------------------------------

@dataclass
class IntegrityResult:
    """Result of a pre-scan integrity check on a single row and column.

    Used by ColumnTypeMigration.pre_scan() to identify rows that cannot
    be cast to new declared types before a schema change is attempted.

    Attributes:
        pk_value:   Primary key value identifying the row, as string.
        column:     Column where the incompatibility was detected.
        reason:     Plain-English description of what went wrong.
        suggestion: Optional hint for how the user can resolve the issue.
        filename:   Source filename — unused here, kept for interface compat.
    """
    pk_value: str | None
    column: str
    reason: str
    suggestion: str | None = None
    filename: str | None = None


# ---------------------------------------------------------------------------
# apply_declared_types
# ---------------------------------------------------------------------------

def apply_declared_types(
    df: pd.DataFrame,
    column_types: dict[str, str],
) -> pd.DataFrame:
    """Cast DataFrame columns to their declared types.

    Used by ColumnTypeMigration._do_swap() and by ingest.py for date
    format columns (DATE|%m-%d-%y) where DuckDB read_csv dtype parameter
    does not support strptime format strings.

    For DATE/TIMESTAMPTZ columns with a format suffix, uses pd.to_datetime
    with the declared format so DuckDB receives proper date values.

    :param df:           The incoming DataFrame.
    :param column_types: {column_name: declared_type} — may include format suffix.
    :return: DataFrame with columns cast to declared types.
    """
    df = df.copy()
    for col, declared_type in column_types.items():
        if col not in df.columns:
            continue

        if "|" in declared_type:
            dt, fmt = declared_type.split("|", 1)
            dt = dt.upper()
        else:
            dt = declared_type.upper()
            fmt = None

        try:
            if dt in ("DOUBLE", "FLOAT", "REAL"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dt in ("BIGINT", "INTEGER", "INT", "SMALLINT", "TINYINT", "HUGEINT"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dt == "BOOLEAN":
                df[col] = df[col].astype(bool)
            elif dt in ("DATE", "TIMESTAMPTZ") and fmt:
                parsed = pd.to_datetime(df[col], format=fmt, errors="coerce")
                if dt == "DATE":
                    df[col] = parsed.dt.date
                else:
                    df[col] = parsed
        except Exception:
            pass

    return df


# ---------------------------------------------------------------------------
# check_type_compatibility
# ---------------------------------------------------------------------------

def check_type_compatibility(
    df: pd.DataFrame,
    reference_types: dict[str, str],
    conn: duckdb.DuckDBPyConnection,
    pk_col: str | None,
) -> list[IntegrityResult]:
    """Check DataFrame values against declared or existing column types.

    Uses DuckDB TRY_CAST to find rows that would fail a type change.
    Used by ColumnTypeMigration.pre_scan() before altering a table schema.

    :param df:              DataFrame to scan (existing table data).
    :param reference_types: {column: declared_type} — may include format suffix.
    :param conn:            Open DuckDB connection.
    :param pk_col:          Primary key column for row identification, or None.
    :return: List of IntegrityResult — one per failing row per column.
    """
    results: list[IntegrityResult] = []

    for col, declared_type in reference_types.items():
        if col not in df.columns or col.startswith("_"):
            continue

        if "|" in declared_type:
            base_type, fmt = declared_type.split("|", 1)
            base_type = base_type.upper()
        else:
            base_type, fmt = declared_type.upper(), None

        pk_select = f'"{pk_col}"' if pk_col and pk_col in df.columns else "NULL"

        try:
            if fmt and base_type in ("DATE", "TIMESTAMPTZ"):
                failing = conn.execute(f"""
                    SELECT {pk_select} AS pk_value,
                           CAST("{col}" AS VARCHAR) AS raw_value
                    FROM df
                    WHERE TRY_STRPTIME(CAST("{col}" AS VARCHAR), '{fmt}') IS NULL
                    AND "{col}" IS NOT NULL
                """).df()
            else:
                failing = conn.execute(f"""
                    SELECT {pk_select} AS pk_value,
                           CAST("{col}" AS VARCHAR) AS raw_value
                    FROM df
                    WHERE TRY_CAST("{col}" AS {base_type}) IS NULL
                    AND "{col}" IS NOT NULL
                """).df()
        except Exception as exc:
            results.append(IntegrityResult(
                pk_value=None,
                column=col,
                reason=f"Invalid declared type '{declared_type}': {exc}",
                suggestion=f"Fix the column_types entry for '{col}'.",
            ))
            continue

        for _, row in failing.iterrows():
            pk_raw = row["pk_value"]
            pk_value = None if (pk_raw is None or pd.isna(pk_raw)) else str(pk_raw)
            fmt_note = f" using format '{fmt}'" if fmt else ""
            results.append(IntegrityResult(
                pk_value=pk_value,
                column=col,
                reason=f"Value '{row['raw_value']}' cannot be cast to {base_type}{fmt_note}.",
                suggestion=(
                    f"Fix the value in the source file, or run 'vp edit source' "
                    f"to update the declared type for '{col}'."
                ),
            ))

    return results


# ---------------------------------------------------------------------------
# auto_migrate
# ---------------------------------------------------------------------------

def auto_migrate(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    df: pd.DataFrame,
) -> list[str]:
    """Add columns present in df but missing from the table.

    New columns are typed based on pandas dtype inference — acceptable here
    since this is a schema ALTER, not a data load operation.
    Does not remove or rename existing columns.

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
        success:         True if migration completed without errors.
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
      execute_force() — flags incompatible rows to source_block, migrates the rest

    :param conn:      Open DuckDB connection.
    :param table:     Target table name.
    :param new_types: {column_name: new_declared_type} — only changed columns
                      needed, but passing all columns is safe.
    :param pk_col:    Primary key column for row identity in source_block, or None.
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

        Returns a list of IntegrityResult for rows that would fail the cast.
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
        """Migrate column types, flagging incompatible rows to source_block."""
        if not self._columns_changed:
            return MigrationResult(success=True)

        issues = self.pre_scan()
        flagged_count = 0

        existing_df = self._conn.execute(
            f'SELECT * FROM "{self._table}"'
        ).df()

        if issues and not existing_df.empty:
            from proto_pipe.pipelines.flagging import FlagRecord, write_source_flags

            flag_records = [
                FlagRecord(
                    id=flag_id_for(i.pk_value),
                    table_name=self._table,
                    check_name="type_conflict",
                    pk_value=i.pk_value,
                    bad_columns=i.column,
                    reason=f"[{i.column}] {i.reason}"[:500],
                )
                for i in issues
            ]
            flagged_count = write_source_flags(self._conn, flag_records)

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


# ---------------------------------------------------------------------------
# Pipeline schema migration — vp db-init --migrate
# ---------------------------------------------------------------------------

def migrate_pipeline_schema(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Apply pending pipeline schema migrations to an existing database.

    Safe to run on any database — each migration checks whether it is
    needed before applying. Returns a list of migration descriptions applied.

    Migrations:
      1. Rename ingest_log → ingest_state (if ingest_log exists)
      2. Rename flagged_rows → source_block (if flagged_rows exists)
      3. Add new columns to source_block (pk_value, source_file, etc.)
      4. CREATE source_pass if missing
      5. CREATE validation_pass if missing
      6. CREATE validation_block if missing

    Called by vp db-init --migrate.
    """
    from proto_pipe.io.db import (
        init_source_pass,
        init_validation_pass,
        init_validation_block,
        table_exists,
    )

    applied: list[str] = []

    # ── 1. ingest_log → ingest_state ─────────────────────────────────────
    if _table_exists(conn, "ingest_log") and not _table_exists(conn, "ingest_state"):
        conn.execute('ALTER TABLE "ingest_log" RENAME TO "ingest_state"')
        applied.append("Renamed ingest_log → ingest_state")
    elif _table_exists(conn, "ingest_log") and _table_exists(conn, "ingest_state"):
        # Both exist — migrate data then drop old
        conn.execute("""
            INSERT INTO ingest_state
            SELECT * FROM ingest_log
            ON CONFLICT (id) DO NOTHING
        """)
        conn.execute('DROP TABLE "ingest_log"')
        applied.append("Merged ingest_log into ingest_state and dropped ingest_log")

    # ── 2. flagged_rows → source_block ───────────────────────────────────
    if _table_exists(conn, "flagged_rows") and not _table_exists(conn, "source_block"):
        # Create source_block with new schema, migrate data from flagged_rows
        conn.execute("""
            CREATE TABLE source_block (
                id                  VARCHAR PRIMARY KEY,
                table_name          VARCHAR NOT NULL,
                check_name          VARCHAR NOT NULL,
                pk_value            VARCHAR,
                source_file         VARCHAR,
                source_file_missing BOOLEAN DEFAULT FALSE,
                bad_columns         VARCHAR,
                reason              VARCHAR,
                flagged_at          TIMESTAMPTZ NOT NULL
            )
        """)
        # Migrate existing rows — new columns default to NULL
        conn.execute("""
            INSERT INTO source_block
                (id, table_name, check_name, reason, flagged_at)
            SELECT id, table_name, check_name, reason, flagged_at
            FROM flagged_rows
            ON CONFLICT (id) DO NOTHING
        """)
        conn.execute('DROP TABLE "flagged_rows"')
        applied.append(
            "Renamed flagged_rows → source_block (added pk_value, source_file, "
            "source_file_missing, bad_columns columns; existing rows migrated)"
        )
    elif _table_exists(conn, "source_block"):
        # source_block exists — ensure new columns are present
        new_col_applied = _ensure_source_block_columns(conn)
        if new_col_applied:
            applied.extend(new_col_applied)

    # ── 3. source_pass — create if missing ───────────────────────────────
    if not _table_exists(conn, "source_pass"):
        init_source_pass(conn)
        applied.append("Created source_pass table")

    # ── 4. validation_pass — create if missing ────────────────────────────
    if not _table_exists(conn, "validation_pass"):
        init_validation_pass(conn)
        applied.append("Created validation_pass table")

    # ── 5. validation_block — create if missing ───────────────────────────
    if not _table_exists(conn, "validation_block"):
        init_validation_block(conn)
        applied.append("Created validation_block table")

    # ── 6. check_registry_metadata.func_name — add if missing ─────────────
    if _table_exists(conn, "check_registry_metadata"):
        existing_cols = set(
            conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'check_registry_metadata'"
            ).df()["column_name"].tolist()
        )
        if "func_name" not in existing_cols:
            conn.execute(
                'ALTER TABLE check_registry_metadata ADD COLUMN func_name VARCHAR'
            )
            applied.append("Added column check_registry_metadata.func_name")

    return applied


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    """Check if a table exists — local helper to avoid circular import."""
    result = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return result is not None and result[0] > 0


def _ensure_source_block_columns(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Add any missing columns to an existing source_block table.

    Handles the case where source_block exists but was created before
    the new schema (missing pk_value, source_file, etc.).
    """
    applied = []
    existing_cols = set(
        conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'source_block'"
        ).df()["column_name"].tolist()
    )

    new_columns = {
        "pk_value":            "VARCHAR",
        "source_file":         "VARCHAR",
        "source_file_missing": "BOOLEAN DEFAULT FALSE",
        "bad_columns":         "VARCHAR",
    }

    for col, sql_type in new_columns.items():
        if col not in existing_cols:
            conn.execute(f'ALTER TABLE source_block ADD COLUMN "{col}" {sql_type}')
            applied.append(f"Added column source_block.{col}")

    return applied
