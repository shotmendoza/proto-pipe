"""Ingestion module.

Scans a directory for CSV and Excel files, matches each file to a source
definition by pattern, and loads the data into DuckDB tables.

Table lifecycle:
- `db-init` — creates the DuckDB file and bootstraps empty tables from sources_config.yaml
- First ingest of a new source — creates the table lazily from the file's schema
- Subsequent ingests — appends rows, auto-migrating new columns if the schema grew

File deduplication:
- Files already logged as 'ok' in ingest_log are skipped automatically.
- Files that previously failed are retried on every run until they succeed.

Flag identity:
- flagged_rows.id = md5(str(pk_value))
- Deterministic: same row always gets the same id.
- ON CONFLICT (id) DO NOTHING makes re-flagging idempotent.
- At export time, the join is:
      flagged_rows.id = md5(CAST(source.pk_col AS VARCHAR))
  computed entirely in DuckDB SQL — no extra columns in source tables.
- Validation check flags (no pk_value) fall back to uuid4.

Duplicate row handling (on_duplicate in sources_config.yaml):
- flag   — (default when primary_key defined) compute md5 row hash; if hash
            differs from existing row, flag the conflict and skip the row
- append — insert all rows regardless of duplicates
- upsert — delete existing rows by primary key, then insert incoming rows
- skip   — silently discard rows whose primary key already exists
"""

import hashlib
import json
import uuid
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd  # type: ignore


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_SUPPORTED_FILE_SUFFIXES = {".csv", ".xlsx", ".xls"}
_MAX_DIFF_COLS = 5

# Number of primary keys processed per SQL IN (...) clause.
CHUNK_SIZE = 1000


# ---------------------------------------------------------------------------
# Flag identity
# ---------------------------------------------------------------------------

def flag_id_for(pk_value) -> str:
    """Return the deterministic flag id for a given primary key value.

    id = md5(str(pk_value))

    Using md5 means the same expression is computable in DuckDB SQL:
        md5(CAST(source.pk_col AS VARCHAR))
    allowing export_flagged to join without any extra stored columns.
    """
    return hashlib.md5(str(pk_value).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Structural checks
# ---------------------------------------------------------------------------

def _is_supported_file(path: Path) -> bool:
    return path.suffix.lower() in _SUPPORTED_FILE_SUFFIXES


def _structural_checks(df: pd.DataFrame, source: dict) -> list[str]:
    """Return a list of structural issues; empty means safe to ingest."""
    issues: list[str] = []
    if df.empty:
        issues.append("File is empty")
    timestamp_col = source.get("timestamp_col")
    if timestamp_col and timestamp_col not in df.columns:
        issues.append(f"Missing required timestamp column '{timestamp_col}'")
    return issues


# ---------------------------------------------------------------------------
# ingest_log table
# ---------------------------------------------------------------------------

def _init_ingest_log(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingest_log (
            id          VARCHAR PRIMARY KEY,
            filename    VARCHAR NOT NULL,
            table_name  VARCHAR,
            status      VARCHAR NOT NULL,
            rows        INTEGER,
            new_cols    VARCHAR,
            message     VARCHAR,
            ingested_at TIMESTAMPTZ NOT NULL
        )
    """)


def _log_ingest(
        conn: duckdb.DuckDBPyConnection,
        filename: str,
        table_name: str | None,
        status: str,
        rows: int | None = None,
        new_cols: list[str] | None = None,
        message: str | None = None
) -> None:
    conn.execute("""
        INSERT INTO ingest_log
            (id, filename, table_name, status, rows, new_cols, message, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        str(uuid.uuid4()),
        filename,
        table_name,
        status,
        rows,
        json.dumps(new_cols) if new_cols else None,
        message,
        datetime.now(timezone.utc),
    ])


# ---------------------------------------------------------------------------
# File deduplication
# ---------------------------------------------------------------------------

def _already_ingested(conn: duckdb.DuckDBPyConnection, filename: str) -> bool:
    """Return True if this filename was already successfully ingested."""
    result = conn.execute("""
        SELECT count(*) FROM ingest_log
        WHERE filename = ? AND status = 'ok'
    """, [filename]).fetchone()
    return result[0] > 0  # type: ignore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def resolve_source(filename: str, sources: list[dict]) -> dict | None:
    for source in sources:
        if any(fnmatch(filename, p) for p in source["patterns"]):
            return source
    return None


def load_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Unsupported file type: {path.suffix}")


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    result = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return result[0] > 0


def _get_existing_columns(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        [table],
    ).fetchall()
    return {row[0] for row in rows}


def _auto_migrate(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> list[str]:
    """Add columns present in df but missing from the table."""
    existing = _get_existing_columns(conn, table)
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
# DB initialisation
# ---------------------------------------------------------------------------

def init_db(db_path: str, sources: list[dict]) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    _init_ingest_log(conn)

    for source in sources:
        table = source["target_table"]
        if _table_exists(conn, table):
            print(f"  [skip] Table '{table}' already exists")
            continue
        conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" (_placeholder VARCHAR)')
        print(f"  [ok]   Created table '{table}'")

    conn.close()


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

def ingest_directory(
    directory: str,
    sources: list[dict],
    db_path: str,
    mode: Literal["append", "replace"] = "append",
    run_checks: bool = False,
    check_registry=None,
    report_registry=None,
) -> dict:
    """Scan a directory, match files to source definitions, load into DuckDB."""
    directory_path = Path(directory)
    if not directory_path.exists():
        raise ValueError(
            f"Incoming directory not found: '{directory}'\n"
            f"Check the incoming_dir setting in pipeline.yaml or pass --incoming-dir."
        )
    if not directory_path.is_dir():
        raise ValueError(f"incoming_dir '{directory}' exists but is not a directory.")

    conn = duckdb.connect(db_path)
    _init_ingest_log(conn)

    summary: dict[str, dict] = {}
    unmatched: list[str] = []

    for path in sorted(directory_path.iterdir()):
        if not _is_supported_file(path):
            continue

        source = resolve_source(path.name, sources)
        if source is None:
            unmatched.append(path.name)
            continue

        if _already_ingested(conn, path.name):
            print(f"[skipped] '{path.name}' — already ingested")
            summary[path.name] = {"status": "skipped", "message": "already ingested"}
            continue

        try:
            df = load_file(path)
        except Exception as exc:
            message = f"Could not load file: {exc}"
            print(f"[fail] '{path.name}': {message}")
            _log_ingest(conn, path.name, None, "failed", message=message)
            summary[path.name] = {"status": "failed", "message": message}
            continue

        issues = _structural_checks(df, source)
        if issues:
            message = "; ".join(issues)
            print(f"[fail] '{path.name}': {message}")
            _log_ingest(conn, path.name, source["target_table"], "failed", message=message)
            summary[path.name] = {"status": "failed", "message": message}
            continue

        table        = source["target_table"]
        primary_key  = source.get("primary_key")
        on_duplicate = source.get("on_duplicate", "flag" if primary_key else "append")

        if mode == "replace" or not _table_exists(conn, table):
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM df')
            new_cols: list[str] = []
            flagged_count = 0
            skipped_count = 0
            print(f"[ok] '{path.name}' → '{table}' ({len(df)} rows, created)")
        else:
            new_cols = _auto_migrate(conn, table, df)

            if primary_key:
                df, flagged_count, skipped_count = _handle_duplicates(
                    conn, table, df, primary_key, on_duplicate
                )
            else:
                if on_duplicate not in ("append", "flag"):
                    print(
                        f"[warn] on_duplicate='{on_duplicate}' ignored for "
                        f"'{table}' — no primary_key defined"
                    )
                flagged_count = 0
                skipped_count = 0

            if not df.empty:
                col_list = ", ".join(f'"{c}"' for c in df.columns)
                conn.execute(
                    f'INSERT INTO "{table}" ({col_list}) SELECT {col_list} FROM df'
                )

            print(
                f"[ok] '{path.name}' → '{table}' "
                f"({len(df)} inserted, {flagged_count} flagged, {skipped_count} skipped)"
            )

        _log_ingest(conn, path.name, table, "ok", rows=len(df), new_cols=new_cols)
        summary[path.name] = {
            "table": table, "rows": len(df), "new_cols": new_cols,
            "flagged": flagged_count, "skipped": skipped_count, "status": "ok",
        }

        if run_checks and check_registry and report_registry:
            _run_inline_checks(conn, table, source, check_registry, report_registry, path.name)

    if unmatched:
        print(f"[warn] No source match for: {', '.join(unmatched)}")
        for filename in unmatched:
            _log_ingest(conn, filename, None, "skipped", message="No matching source pattern")

    conn.close()
    return summary


def _run_inline_checks(conn, table, source, check_registry, report_registry, filename):
    matching = [
        r for r in report_registry.all()
        if r.get("source", {}).get("table") == table
    ]
    if not matching:
        return
    df      = conn.execute(f'SELECT * FROM "{table}"').df()
    context = {"df": df}
    for report in matching:
        for check_name in report.get("resolved_checks", []):
            try:
                result = check_registry.run(check_name, context)
                print(f"[check] {check_name}: {result}")
            except Exception as e:
                print(f"[check-fail] {check_name}: {e}")


# ---------------------------------------------------------------------------
# Duplicate row handling
# ---------------------------------------------------------------------------

def _row_hash_expr(cols: list[str]) -> str:
    """DuckDB md5() expression over all given columns for content hashing."""
    parts = " || '|' || ".join(
        [f"COALESCE(CAST(\"{c}\" AS VARCHAR), '')" for c in cols]
    )
    return f"md5({parts})"


def _write_flag(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    diffs: list[str],
    pk_value=None,
) -> None:
    """Insert one conflict entry into flagged_rows.

    id = md5(str(pk_value)) when pk_value is provided — deterministic,
    computable in DuckDB SQL at export time, no extra columns needed.

    Falls back to uuid4 when pk_value is None (validation check flags).
    ON CONFLICT (id) DO NOTHING makes this idempotent.
    """
    shown    = diffs[:_MAX_DIFF_COLS]
    leftover = len(diffs) - len(shown)
    reason   = " | ".join(shown) + (f" +{leftover} more" if leftover else "")
    fid      = (
        flag_id_for(pk_value)
        if pk_value is not None
        else str(uuid.uuid4())
    )
    conn.execute("""
        INSERT INTO flagged_rows
            (id, table_name, check_name, reason, flagged_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (id) DO NOTHING
    """, [fid, table_name, "duplicate_conflict", reason, datetime.now(timezone.utc)])


def _changed_columns(
    existing_row: pd.Series,
    incoming_row: pd.Series,
    cols: list[str],
) -> list[str]:
    """Return 'col: old -> new' diff strings for columns that changed."""
    diffs = []
    for col in cols:
        old_val = existing_row.get(col)
        new_val = incoming_row.get(col)
        try:
            old_null = pd.isna(old_val)
            new_null = pd.isna(new_val)
        except (TypeError, ValueError):
            old_null = old_val is None
            new_null = new_val is None
        if old_null and new_null:
            continue
        if old_null != new_null or old_val != new_val:
            old_str = "NULL" if old_null else str(old_val)
            new_str = "NULL" if new_null else str(new_val)
            diffs.append(f"{col}: {old_str} -> {new_str}")
    return diffs


def _handle_duplicates(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    df: pd.DataFrame,
    primary_key: str,
    on_duplicate: str,
) -> tuple[pd.DataFrame, int, int]:
    """Process incoming rows against existing rows by primary key.

    Processes keys in chunks of CHUNK_SIZE to avoid large IN (...) clauses.
    Returns (rows_to_insert, flagged_count, skipped_count).
    """
    if on_duplicate == "append":
        return df, 0, 0

    if primary_key not in df.columns:
        print(
            f"  [warn] primary_key '{primary_key}' not in incoming columns "
            f"— appending all rows"
        )
        return df, 0, 0

    null_key_count = df[primary_key].isna().sum()
    if null_key_count:
        print(
            f"  [warn] {null_key_count} row(s) have NULL primary key "
            f"'{primary_key}' — bypassing duplicate detection"
        )

    valid_df  = df[df[primary_key].notna()].copy()
    null_rows = df[df[primary_key].isna()].copy()

    if valid_df.empty:
        return df, 0, 0

    non_key_cols = [c for c in valid_df.columns if c != primary_key]
    hash_cols    = [c for c in non_key_cols if c in _get_existing_columns(conn, table)]
    hash_expr    = _row_hash_expr(hash_cols) if hash_cols else "md5('')"

    all_rows_to_insert: list = []
    total_flagged = 0
    total_skipped = 0

    for chunk_start in range(0, len(valid_df), CHUNK_SIZE):
        chunk        = valid_df.iloc[chunk_start: chunk_start + CHUNK_SIZE]
        chunk_keys   = chunk[primary_key].tolist()
        placeholders = ", ".join(["?"] * len(chunk_keys))

        existing_df = conn.execute(
            f'SELECT * FROM "{table}" WHERE "{primary_key}" IN ({placeholders})',
            chunk_keys,
        ).df()

        if existing_df.empty:
            all_rows_to_insert.extend(chunk.itertuples(index=False, name=None))
            continue

        if on_duplicate == "upsert":
            conn.execute(
                f'DELETE FROM "{table}" WHERE "{primary_key}" IN ({placeholders})',
                chunk_keys,
            )
            all_rows_to_insert.extend(chunk.itertuples(index=False, name=None))
            continue

        if on_duplicate == "skip":
            existing_keys = set(existing_df[primary_key].tolist())
            keep    = chunk[~chunk[primary_key].isin(existing_keys)]
            skipped = len(chunk) - len(keep)
            if skipped:
                print(f"  [skip] {skipped} row(s) already exist — skipped")
            all_rows_to_insert.extend(keep.itertuples(index=False, name=None))
            total_skipped += skipped
            continue

        if on_duplicate == "flag":
            if not hash_cols:
                all_rows_to_insert.extend(chunk.itertuples(index=False, name=None))
                continue

            existing_hashes = (
                conn.execute(
                    f'SELECT "{primary_key}", {hash_expr} AS row_hash '
                    f'FROM "{table}" WHERE "{primary_key}" IN ({placeholders})',
                    chunk_keys,
                )
                .df()
                .groupby(primary_key)["row_hash"]
                .first()
                .to_dict()
            )

            incoming_hashes = (
                conn.execute(
                    f'SELECT "{primary_key}", {hash_expr} AS row_hash FROM chunk '
                    f'WHERE "{primary_key}" IN ({placeholders})',
                    chunk_keys,
                )
                .df()
                .set_index(primary_key)["row_hash"]
                .to_dict()
            )

            existing_by_key: dict = {}
            for _, row in existing_df.iterrows():
                key_val = row[primary_key]
                if key_val not in existing_by_key:
                    existing_by_key[key_val] = row

            dup_existing = len(existing_df) - len(existing_by_key)
            if dup_existing:
                print(
                    f"  [warn] {dup_existing} duplicate primary key(s) in existing "
                    f"table — using first occurrence for conflict comparison"
                )

            for _, incoming_row in chunk.iterrows():
                key_val = incoming_row[primary_key]

                if key_val not in existing_hashes:
                    all_rows_to_insert.append(incoming_row)
                    continue

                if existing_hashes.get(key_val) == incoming_hashes.get(key_val):
                    continue

                existing_row = existing_by_key.get(key_val)
                changed = (
                    _changed_columns(existing_row, incoming_row, hash_cols)
                    if existing_row is not None
                    else [f"{c}: (unknown) -> {incoming_row.get(c)}" for c in hash_cols]
                )

                _write_flag(conn, table, changed, pk_value=key_val)
                total_flagged += 1
                print(
                    f"  [flag] key={key_val} — "
                    f"{' | '.join(changed[:3])}{'...' if len(changed) > 3 else ''}"
                )

            continue

        print(f"  [warn] Unknown on_duplicate '{on_duplicate}' — appending chunk")
        all_rows_to_insert.extend(chunk.itertuples(index=False, name=None))

    if total_flagged:
        print(f"  [flag] {total_flagged} conflict(s) flagged — original rows kept")

    result_parts = []
    if all_rows_to_insert:
        result_parts.append(pd.DataFrame(all_rows_to_insert, columns=valid_df.columns))
    if not null_rows.empty:
        result_parts.append(null_rows)

    result = (
        pd.concat(result_parts, ignore_index=True)
        if result_parts
        else pd.DataFrame(columns=df.columns)
    )
    return result, total_flagged, total_skipped


def check_null_overwrites(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    primary_key: str,
) -> int:
    """Scan for primary keys with multiple rows of differing content and flag conflicts.

    Idempotent — md5(pk_value) + ON CONFLICT DO NOTHING means running twice is safe.
    Returns the number of INSERT attempts (includes skipped duplicates).
    """
    duplicate_keys = conn.execute(f"""
        SELECT "{primary_key}"
        FROM "{table}"
        GROUP BY "{primary_key}"
        HAVING count(*) > 1
    """).df()

    if duplicate_keys.empty:
        return 0

    all_cols     = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        [table],
    ).df()["column_name"].tolist()
    non_key_cols = [c for c in all_cols if c != primary_key]

    keys    = duplicate_keys[primary_key].tolist()
    flagged = 0

    for chunk_start in range(0, len(keys), CHUNK_SIZE):
        chunk_keys   = keys[chunk_start: chunk_start + CHUNK_SIZE]
        placeholders = ", ".join(["?"] * len(chunk_keys))
        rows = conn.execute(
            f'SELECT * FROM "{table}" WHERE "{primary_key}" IN ({placeholders})',
            chunk_keys,
        ).df()

        for key_val, group in rows.groupby(primary_key):
            if len(group) < 2:
                continue
            first = group.iloc[0]
            for _, later in group.iloc[1:].iterrows():
                changed = _changed_columns(first, later, non_key_cols)
                if not changed:
                    continue
                _write_flag(conn, table, changed, pk_value=key_val)
                flagged += 1

    return flagged
