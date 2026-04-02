"""Report runner — validation and deliverable execution.

Validation flow (vp validate):
  1. Compute check_set_hash for the report
  2. Compare against validation_pass to find pending records
  3. Create report table on first run (copy of source), upsert on subsequent
  4. Run kind='check' functions → failures to validation_block
  5. Run kind='transform' functions → TRY_CAST gate → UPDATE report table
  6. Update validation_pass for all processed records
  7. Advance watermark only when all checks passed

Report table lifecycle:
  - Named in report config as target_table
  - First vp validate: CREATE TABLE AS SELECT from source (full copy)
  - Subsequent runs: upsert only new/changed records per validation_pass
  - Source table is never modified by this layer
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timezone, datetime
from pathlib import Path

import duckdb
import pandas as pd

from proto_pipe.checks.runner import run_checks
from proto_pipe.io.data import load_from_duckdb
from proto_pipe.io.registry import resolve_filename, write_xlsx_sheet, write_csv
from proto_pipe.pipelines.watermark import WatermarkStore, _watermark_lock
from proto_pipe.checks.registry import CheckRegistry, ReportRegistry, CheckParamInspector
from proto_pipe.reports.query import _log_run, init_report_runs_table


# ---------------------------------------------------------------------------
# Transform application — with TRY_CAST gate
# ---------------------------------------------------------------------------

def _apply_transforms_with_gate(
    transform_names: list[str],
    check_registry: CheckRegistry,
    df: pd.DataFrame,
    target_table: str,
    report_name: str,
    pk_col: str | None,
    conn: duckdb.DuckDBPyConnection,
    registry_types: dict[str, str],
) -> None:
    """Apply transforms with a TRY_CAST gate before writing back to DuckDB.

    For each transform:
      - Scalar transform (no pd.Series/pd.DataFrame params) → attempt DuckDB
        Python UDF registration; fall back to pandas on failure.
      - Series/DataFrame transform → pandas execution → TRY_CAST gate in DuckDB
        → UPDATE clean rows in report table, bad rows to validation_block.

    A single UPDATE per transform (not DROP+CREATE) so the report table is
    never fully replaced — only affected rows are touched.
    """
    from proto_pipe.pipelines.flagging import FlagRecord, write_validation_flags
    from proto_pipe.io.db import flag_id_for

    for name in transform_names:
        try:
            func = check_registry.get(name)
            inspector = CheckParamInspector(func) if func else None

            # Route: scalar (no Series/DataFrame params) vs Series/DataFrame
            is_scalar = (
                inspector is not None
                and not inspector.column_params()
                and not inspector.dataframe_params()
            )

            if is_scalar:
                # Try DuckDB Python UDF path
                try:
                    _apply_scalar_transform_duckdb(
                        name, func, df, target_table, conn, registry_types
                    )
                    continue
                except Exception as udf_exc:
                    print(
                        f"  [transform] '{name}' DuckDB UDF failed ({udf_exc}) "
                        f"— falling back to pandas"
                    )

            # Pandas execution path
            result = check_registry.run(name, {"df": df})

            if isinstance(result, pd.DataFrame):
                modified_df = result.copy()
            elif isinstance(result, pd.Series):
                col_name = result.name
                if not col_name or col_name not in df.columns:
                    print(
                        f"  [transform-warn] '{name}' returned Series "
                        f"with name={col_name!r} — not found in table, skipped"
                    )
                    continue
                modified_df = df.copy()
                modified_df[col_name] = result.values
            else:
                print(
                    f"  [transform-warn] '{name}' returned {type(result).__name__} "
                    f"(expected DataFrame or Series) — skipped"
                )
                continue

            # TRY_CAST gate — validate modified columns against declared types
            user_cols = [c for c in modified_df.columns if not c.startswith("_")]
            affected_types = {
                col: rt for col, rt in registry_types.items() if col in user_cols
            }

            blocked_pks: set[str] = set()
            if affected_types and pk_col and pk_col in modified_df.columns:
                for col, declared_type in affected_types.items():
                    if col not in modified_df.columns:
                        continue
                    base_type = (
                        declared_type.split("|")[0].upper()
                        if "|" in declared_type
                        else declared_type.upper()
                    )
                    try:
                        failing = conn.execute(f"""
                            SELECT CAST("{pk_col}" AS VARCHAR) AS pk_val,
                                   CAST("{col}" AS VARCHAR) AS raw_value
                            FROM modified_df
                            WHERE TRY_CAST("{col}" AS {base_type}) IS NULL
                            AND "{col}" IS NOT NULL
                        """).df()
                    except Exception:
                        continue

                    for _, row in failing.iterrows():
                        pk_str = str(row["pk_val"])
                        blocked_pks.add(pk_str)
                        flag_records = [FlagRecord(
                            id=flag_id_for(pk_str),
                            table_name=target_table,
                            report_name=report_name,
                            check_name=name,
                            pk_value=pk_str,
                            bad_columns=col,
                            reason=(
                                f"[{col}] transform '{name}' produced "
                                f"'{row['raw_value']}' which cannot be cast to {base_type}"
                            )[:500],
                        )]
                        write_validation_flags(conn, flag_records)

                if blocked_pks:
                    print(
                        f"  [transform-warn] '{name}' produced {len(blocked_pks)} "
                        f"row(s) that failed TRY_CAST → validation_block"
                    )
                    modified_df = modified_df[
                        ~modified_df[pk_col].astype(str).isin(blocked_pks)
                    ].copy()

            if modified_df.empty:
                continue

            # UPDATE clean rows in report table — no DROP+CREATE
            _update_report_table(conn, target_table, modified_df, pk_col)
            df = modified_df  # carry forward for subsequent transforms

        except Exception as e:
            print(f"  [transform-fail] '{name}': {e} — skipped, table unchanged")

    print(
        f"  [transform] {len(transform_names)} transform(s) applied to '{target_table}'"
    )


def _apply_scalar_transform_duckdb(
    name: str,
    func,
    df: pd.DataFrame,
    target_table: str,
    conn: duckdb.DuckDBPyConnection,
    registry_types: dict[str, str],
) -> None:
    """Apply a scalar transform by registering as a DuckDB Python UDF.

    Scalar transforms have no pd.Series or pd.DataFrame params — they operate
    value-by-value. This keeps the operation inside DuckDB without a pandas
    round-trip.

    Raises on failure so the caller can fall back to pandas.
    """
    import inspect

    # Get the scalar column this transform operates on from the function signature
    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    if not params:
        raise ValueError(f"Scalar transform '{name}' has no parameters")

    col_param = params[0].name  # first param assumed to be the input column
    if col_param not in df.columns:
        raise ValueError(f"Column '{col_param}' not in DataFrame")

    # Register UDF and run UPDATE in DuckDB
    udf_name = f"_udf_{name}"
    try:
        conn.create_function(udf_name, func)
        conn.execute(
            f'UPDATE "{target_table}" SET "{col_param}" = {udf_name}("{col_param}")'
        )
    finally:
        try:
            conn.remove_function(udf_name)
        except Exception:
            pass


def _update_report_table(
    conn: duckdb.DuckDBPyConnection,
    target_table: str,
    df: pd.DataFrame,
    pk_col: str | None,
) -> None:
    """UPDATE rows in the report table from df. Falls back to INSERT for new rows.

    Uses a staging temp table to batch the UPDATE in one SQL statement.
    """
    if df.empty:
        return

    conn.execute(
        "CREATE TEMP TABLE IF NOT EXISTS _transform_staging AS "
        f'SELECT * FROM "{target_table}" LIMIT 0'
    )
    conn.execute("DELETE FROM _transform_staging")
    conn.execute("INSERT INTO _transform_staging SELECT * FROM df")

    update_cols = [
        c for c in df.columns
        if c != pk_col and not c.startswith("_")
    ]

    if pk_col and update_cols:
        set_clause = ", ".join(
            f'"{c}" = _transform_staging."{c}"' for c in update_cols
        )
        conn.execute(f"""
            UPDATE "{target_table}"
            SET {set_clause}
            FROM _transform_staging
            WHERE "{target_table}"."{pk_col}" = _transform_staging."{pk_col}"
        """)

    conn.execute("DROP TABLE IF EXISTS _transform_staging")


# Legacy transform apply (kept for when pipeline_db is not provided)
def _apply_transforms(
    transform_names: list[str],
    check_registry: CheckRegistry,
    df: pd.DataFrame,
    table_name: str,
    pipeline_db: str,
) -> None:
    """Legacy transform application — full table replacement.

    Used only when pipeline_db is provided but validation_pass tracking is
    not active (i.e. the report was run without a target_table configured).
    """
    working_df = df.copy()
    failed: list[str] = []

    for name in transform_names:
        try:
            result = check_registry.run(name, {"df": working_df})
            if isinstance(result, pd.DataFrame):
                overwrite_cols = result.attrs.get("overwrite_cols")
                if overwrite_cols:
                    working_df = working_df.copy()
                    for col in overwrite_cols:
                        if col in result.columns:
                            working_df[col] = result[col].values
                else:
                    working_df = result
            elif isinstance(result, pd.Series):
                col_name = result.name
                if col_name and col_name in working_df.columns:
                    working_df = working_df.copy()
                    working_df[col_name] = result.values
        except Exception as e:
            print(f"  [transform-fail] '{name}': {e} — skipped")
            failed.append(name)

    conn = duckdb.connect(pipeline_db)
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM working_df')
    finally:
        conn.close()

    print(
        f"  [transform] {len(transform_names) - len(failed)}/{len(transform_names)} "
        f"transform(s) applied to '{table_name}'"
    )


# ---------------------------------------------------------------------------
# Report execution
# ---------------------------------------------------------------------------

def run_report(
    report_config: dict,
    check_registry: CheckRegistry,
    watermark_store: WatermarkStore,
    pipeline_db: str | None = None,
    full_revalidation: bool = False,
) -> dict:
    """Execute a report — run checks and transforms against the source table.

    Validation flow:
      1. Compute check_set_hash; notify if changed since last run
      2. Find pending records via validation_pass (new, changed, or hash-changed)
      3. Create/upsert report table from source data
      4. Run checks → failures to validation_block
      5. Run transforms → TRY_CAST gate → UPDATE report table
      6. Update validation_pass for all processed records
      7. Advance watermark on full pass

    :param report_config:      Report definition from reports_config.yaml.
    :param check_registry:     Registry of check/transform functions.
    :param watermark_store:    Watermark state store.
    :param pipeline_db:        Path to pipeline.db. Required for flag writing
                               and validation_pass tracking.
    :param full_revalidation:  When True, revalidates all records regardless
                               of validation_pass state. Triggered by --full flag
                               or check_set_hash change.
    :return: {report, status, results} dict.
    """
    report_name = report_config["name"]
    source_config = report_config["source"]
    options = report_config.get("options", {})
    parallel_checks = options.get("parallel", False)

    source_table = source_config.get("table", "")
    pk_col = source_config.get("primary_key")

    # target_table: the physical report table — defaults to <source>_report
    target_table = report_config.get(
        "target_table",
        f"{source_table}_report" if source_table else report_name,
    )

    # ── No pipeline_db — legacy path ─────────────────────────────────────
    if not pipeline_db:
        last_run_at = watermark_store.get(report_name)
        try:
            df = load_from_duckdb(source_config, last_run_at)
        except Exception as e:
            return {"report": report_name, "status": "error", "error": str(e)}
        if df.empty:
            return {"report": report_name, "status": "skipped"}

        all_names = report_config.get("resolved_checks", [])
        check_names = [n for n in all_names if check_registry.get_kind(n) == "check"]
        context = {"df": df}
        results = run_checks(check_names, check_registry, context, parallel=parallel_checks)
        return {"report": report_name, "status": "completed", "results": results}

    # ── Full validation path ──────────────────────────────────────────────
    from proto_pipe.pipelines.flagging import (
        FlagRecord,
        write_validation_flags,
        compute_check_set_hash,
        compute_row_hash,
    )
    from proto_pipe.io.db import (
        flag_id_for,
        get_validation_pass_hashes,
        get_current_check_set_hash,
        upsert_validation_pass,
        get_registry_types,
        table_exists,
    )

    conn = duckdb.connect(pipeline_db)
    try:
        # 1. Compute check_set_hash
        check_entries = report_config.get("checks", [])
        current_hash = compute_check_set_hash(check_entries, check_registry)

        # 2. Detect check set changes
        prior_hash = get_current_check_set_hash(conn, report_name)
        if prior_hash and prior_hash != current_hash:
            print(
                f"  [warn] Check set changed for '{report_name}'. "
                f"Run 'vp validate --report {report_name} --full' to revalidate all records."
            )
            full_revalidation = True

        # 3. Load full source table
        try:
            source_df = conn.execute(f'SELECT * FROM "{source_table}"').df()
        except Exception as e:
            return {"report": report_name, "status": "error", "error": str(e)}

        if source_df.empty:
            return {"report": report_name, "status": "skipped"}

        # 4. Identify pending records via validation_pass
        comparable_cols = [
            c for c in source_df.columns
            if not c.startswith("_") and c != pk_col
        ]

        if pk_col and pk_col in source_df.columns:
            incoming = {
                str(row[pk_col]): compute_row_hash(row.to_dict(), comparable_cols)
                for _, row in source_df.iterrows()
            }
        else:
            incoming = {str(i): "" for i in range(len(source_df))}

        if full_revalidation:
            pending_pks = set(incoming.keys())
        else:
            existing_hashes = get_validation_pass_hashes(
                conn, source_table, report_name, current_hash
            )
            pending_pks = {
                pk for pk, h in incoming.items()
                if pk not in existing_hashes or existing_hashes[pk] != h
            }

        if not pending_pks:
            print(f"  [skip] '{report_name}' — no pending records")
            return {"report": report_name, "status": "skipped"}

        # 5. Get pending records df
        if pk_col and pk_col in source_df.columns:
            pending_df = source_df[
                source_df[pk_col].astype(str).isin(pending_pks)
            ].copy()
        else:
            pending_df = source_df.copy()

        print(
            f"  [{report_name}] {len(pending_pks)} pending record(s) of "
            f"{len(source_df)} total"
        )

        # 6. Create or update report table
        if not table_exists(conn, target_table):
            # First run — CREATE from full source
            conn.execute(f'CREATE TABLE "{target_table}" AS SELECT * FROM source_df')
            print(f"  [ok] Created report table '{target_table}' ({len(source_df)} rows)")
        else:
            # Subsequent run — upsert pending records
            if pk_col and pk_col in pending_df.columns and not pending_df.empty:
                placeholders = ", ".join(["?"] * len(pending_pks))
                conn.execute(
                    f'DELETE FROM "{target_table}" '
                    f'WHERE CAST("{pk_col}" AS VARCHAR) IN ({placeholders})',
                    list(pending_pks),
                )
                col_list = ", ".join(f'"{c}"' for c in pending_df.columns)
                conn.execute(
                    f'INSERT INTO "{target_table}" ({col_list}) '
                    f'SELECT {col_list} FROM pending_df'
                )

        # 7. Run checks against pending records
        all_names = report_config.get("resolved_checks", [])
        check_names = [n for n in all_names if check_registry.get_kind(n) == "check"]
        transform_names = [
            n for n in all_names if check_registry.get_kind(n) == "transform"
        ]

        context = {"df": pending_df}
        results: dict[str, dict] = {}

        for check_name in check_names:
            try:
                result = check_registry.run(check_name, context)
                if isinstance(result, pd.Series) and result.dtype == bool:
                    failing_mask = ~result
                    failing_count = int(failing_mask.sum())

                    if failing_count > 0 and pk_col and pk_col in pending_df.columns:
                        failing_df = pending_df[failing_mask]
                        flag_records = [
                            FlagRecord(
                                id=flag_id_for(str(row[pk_col])),
                                table_name=target_table,
                                report_name=report_name,
                                check_name=check_name,
                                pk_value=str(row[pk_col]),
                                reason=f"Check '{check_name}' failed",
                            )
                            for _, row in failing_df.iterrows()
                        ]
                        write_validation_flags(conn, flag_records)
                        print(
                            f"  [check] '{check_name}' — "
                            f"{failing_count} failure(s) → validation_block"
                        )

                    results[check_name] = {
                        "status": "failed" if failing_count > 0 else "passed",
                        "failed_count": failing_count,
                    }
                else:
                    results[check_name] = {"status": "passed", "failed_count": 0}

            except Exception as e:
                print(f"  [check-fail] '{check_name}': {e}")
                results[check_name] = {"status": "error", "error": str(e)}

        # 8. Apply transforms with TRY_CAST gate
        if transform_names:
            registry_types = get_registry_types(conn, list(pending_df.columns))
            _apply_transforms_with_gate(
                transform_names=transform_names,
                check_registry=check_registry,
                df=pending_df,
                target_table=target_table,
                report_name=report_name,
                pk_col=pk_col,
                conn=conn,
                registry_types=registry_types,
            )

        # 9. Update validation_pass for all processed records
        all_passed = all(
            v.get("status") == "passed" for v in results.values()
        )
        for pk_str, row_hash in incoming.items():
            if pk_str in pending_pks:
                # Per-record status: failed if any check flagged this specific row
                status = "passed"
                if not all_passed:
                    # Check if this PK was specifically flagged
                    flagged = conn.execute(
                        "SELECT count(*) FROM validation_block "
                        "WHERE report_name = ? AND pk_value = ?",
                        [report_name, pk_str],
                    ).fetchone()[0]
                    status = "failed" if flagged > 0 else "passed"

                upsert_validation_pass(
                    conn, source_table, report_name,
                    pk_str, row_hash, current_hash, status
                )

        # 10. Advance watermark on full pass
        if all_passed:
            ts_col = source_config.get("timestamp_col") or "_ingested_at"
            if ts_col and ts_col in source_df.columns:
                max_ts = source_df[ts_col].max()
                if not isinstance(max_ts, datetime):
                    max_ts = datetime.fromisoformat(str(max_ts)).replace(
                        tzinfo=timezone.utc
                    )
                watermark_store.set(report_name, max_ts)

        return {"report": report_name, "status": "completed", "results": results}

    finally:
        conn.close()


def _advance_watermark(
    report_name: str,
    timestamp_col: str,
    df: pd.DataFrame,
    watermark_store: WatermarkStore,
) -> None:
    """Advance the report watermark to the latest timestamp in the loaded data."""
    ts_series = pd.to_datetime(df[timestamp_col], utc=True)
    last_processed_ts = ts_series.max().to_pydatetime()
    with _watermark_lock:
        watermark_store.set(report_name, last_processed_ts)


# ---------------------------------------------------------------------------
# Deliverable runner — unchanged
# ---------------------------------------------------------------------------

def run_deliverable(
    deliverable: dict,
    report_dataframes: dict[str, pd.DataFrame],
    output_dir: str,
    pipeline_db_path: str,
    cli_overrides: dict | None = None,
    run_date: str | None = None,
) -> list[str]:
    """Write output files for a deliverable and log each report run."""
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fmt = deliverable.get("format", "csv")
    name = deliverable["name"]
    template = deliverable["filename_template"]
    out_dir = Path(deliverable.get("output_dir", output_dir))
    reports = deliverable["reports"]

    conn = duckdb.connect(pipeline_db_path)
    init_report_runs_table(conn)

    written = []

    if fmt == "xlsx":
        sheets = {}
        filename = resolve_filename(template, name, run_date)
        output_path = out_dir / filename

        for report_cfg in reports:
            report_name = report_cfg["name"]
            sheet = report_cfg.get("sheet", report_name)
            df = report_dataframes.get(report_name, pd.DataFrame())
            sheets[sheet] = df
            _log_run(
                conn, name, report_name, filename, str(out_dir),
                report_cfg.get("filters"), len(df), fmt, run_date,
            )

        write_xlsx_sheet(sheets, output_path)
        written.append(str(output_path))
        print(
            f"  [ok] {output_path} "
            f"({sum(len(d) for d in sheets.values())} total rows)"
        )

    else:
        for report_cfg in reports:
            report_name = report_cfg["name"]
            filename = resolve_filename(template, report_name, run_date)
            output_path = out_dir / filename
            df = report_dataframes.get(report_name, pd.DataFrame())

            write_csv(df, output_path)
            _log_run(
                conn, name, report_name, filename, str(out_dir),
                report_cfg.get("filters"), len(df), fmt, run_date,
            )
            written.append(str(output_path))
            print(f"  [ok] {output_path} ({len(df)} rows)")

    conn.close()
    return written


# ---------------------------------------------------------------------------
# Top-level runner
# ---------------------------------------------------------------------------

def run_all_reports(
    report_registry: ReportRegistry,
    check_registry: CheckRegistry,
    watermark_store: WatermarkStore,
    parallel_reports: bool = True,
    pipeline_db: str | None = None,
    full_revalidation: bool = False,
) -> list[dict]:
    """Execute all reports. Supports sequential and parallel execution."""
    reports = report_registry.all()

    if not parallel_reports:
        return [
            run_report(
                r, check_registry, watermark_store,
                pipeline_db=pipeline_db,
                full_revalidation=full_revalidation,
            )
            for r in reports
        ]

    results = []
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(
                run_report,
                r,
                check_registry,
                watermark_store,
                pipeline_db=pipeline_db,
                full_revalidation=full_revalidation,
            ): r["name"]
            for r in reports
        }
        for future in as_completed(futures):
            results.append(future.result())
    return results
