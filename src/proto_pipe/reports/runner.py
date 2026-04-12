"""Report runner — validation execution (compute + write split).

Validation flow (vp validate):
  1. Compute check_set_hash for the report
  2. Compare against validation_pass to find pending records
  3. Run kind='check' functions → failures accumulated as FlagRecord objects
  4. Run kind='transform' functions → TRY_CAST gate via :memory: DuckDB → bad rows accumulated
  5. Write report table, validation_block, validation_pass (sequential, one connection)
  6. Advance watermark only when all checks passed

Parallel execution model:
  _compute_report — read-only, parallelisable. Opens short-lived read connection,
                     runs all checks/transforms in memory, returns ReportBundle.
  _write_report — sequential, shared connection. Persists ReportBundle to DuckDB.
  run_all_reports — Phase 1: parallel _compute_report per layer via ThreadPoolExecutor.
                     Phase 2: sequential _write_report with one shared connection.

Report table lifecycle:
  - Named in report config as target_table
  - First vp validate: CREATE TABLE AS SELECT from source, then UPDATE with transform results
  - Subsequent runs: DELETE pending PKs, INSERT modified_df (transforms already applied)
  - Source table never modified by this layer

Structured logging:
  _compute_report appends LogEntry to ReportBundle.log (thread-safe — each
  thread owns its bundle). The orchestrator drains log entries through
  ReportCallback.on_log() on the main thread between compute and write phases.
  _write_report calls ReportCallback.on_write_done() directly (always sequential).

No print() — reports/ never formats output.
No CLI imports — reports/ never imports click, rich, or questionary.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import timezone, datetime
from pathlib import Path

import duckdb
import pandas as pd

from proto_pipe.checks.result import CheckOutcome
from proto_pipe.pipelines.watermark import WatermarkStore, watermark_lock
from proto_pipe.checks.registry import CheckRegistry, ReportRegistry
from proto_pipe.reports.callbacks import LogEntry, ReportCallback


@dataclass
class ReportBundle:
    """In-memory result of the compute phase for one report.

    Produced by _compute_report (read-only, parallelisable).
    Consumed by _write_report (sequential, single shared connection).

    Separating compute from write allows reports in the same execution
    layer to run their checks and transforms concurrently without
    contending on the DuckDB file lock.
    """
    report_name: str
    status: str                          # "computed" | "skipped" | "error"
    error: str | None = None
    results: dict = field(default_factory=dict)   # check outcomes for CLI display

    # Report table management
    target_table: str | None = None
    source_table: str | None = None
    row_count: int = 0               # total rows in source table — for display only
    modified_df: pd.DataFrame | None = None  # pending records post-transform
    first_run: bool = False
    pending_pks: set = field(default_factory=set)
    pk_col: str | None = None

    # Flags and pass state accumulated during compute — not yet written to DB
    flag_records: list = field(default_factory=list)   # list[FlagRecord]
    pass_entries: list = field(default_factory=list)   # list[dict]

    # Watermark
    all_passed: bool = False
    timestamp_col: str | None = None
    current_hash: str | None = None

    # Structured log — appended during compute, drained by orchestrator
    log: list[LogEntry] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Dependency inference helpers
# ---------------------------------------------------------------------------

def get_target_table(report_config: dict) -> str:
    """Compute the output table name for a report config.

    Uses explicit target_table if set in config, otherwise defaults to
    <source_table>_report. This mirrors the logic in run_report and is the
    canonical rule used for dependency inference across run_all_reports and
    _build_execution_layers.
    """
    source_table = report_config.get("source", {}).get("table", "")
    return report_config.get(
        "target_table",
        f"{source_table}_report" if source_table else report_config["name"],
    )


def _build_execution_layers(reports: list[dict]) -> list[list[dict]]:
    """Topologically sort reports into execution layers by inferred dependency.

    Dependency rule
    ---------------
    Report B depends on report A when B.source.table matches A's target_table.
    target_table is resolved via get_target_table: explicit config value first,
    falling back to '<source_table>_report'.

    Execution contract
    ------------------
    - Reports in the same layer have no dependencies on each other and may
      run concurrently.
    - Every layer must complete before the next layer begins.
    - Reports with no dependencies are always in layer 0.

    Raises
    ------
    ValueError
        If a dependency cycle is detected, naming the reports involved.
    """
    name_to_report: dict[str, dict] = {r["name"]: r for r in reports}
    target_to_name: dict[str, str] = {get_target_table(r): r["name"] for r in reports}
    dependencies: dict[str, set[str]] = {r["name"]: set() for r in reports}
    dependents: dict[str, set[str]] = {r["name"]: set() for r in reports}

    for r in reports:
        source_table = r.get("source", {}).get("table", "")
        upstream_name = target_to_name.get(source_table)
        if upstream_name and upstream_name != r["name"]:
            dependencies[r["name"]].add(upstream_name)
            dependents[upstream_name].add(r["name"])

    in_degree: dict[str, int] = {name: len(deps) for name, deps in dependencies.items()}
    queue: list[str] = [name for name, deg in in_degree.items() if deg == 0]
    layers: list[list[dict]] = []

    while queue:
        layer = [name_to_report[name] for name in queue]
        layers.append(layer)
        next_queue: list[str] = []
        for name in queue:
            for dependent in dependents[name]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    next_queue.append(dependent)
        queue = next_queue

    scheduled = sum(len(layer) for layer in layers)
    if scheduled < len(reports):
        cycled = sorted(name for name, deg in in_degree.items() if deg > 0)
        raise ValueError(
            f"Dependency cycle detected among reports: {', '.join(cycled)}. "
            f"Check source.table and target_table config for circular references."
        )

    return layers


def _update_report_table(
    conn: duckdb.DuckDBPyConnection,
    target_table: str,
    df: pd.DataFrame,
    pk_col: str | None,
) -> None:
    """UPDATE existing rows in the report table from df via staging.

    Uses a staging temp table to batch the UPDATE in one SQL statement.
    This is intentionally separate from upsert_via_staging in io/db.py:
    upsert_via_staging does INSERT...ON CONFLICT (idempotent upsert);
    this function does UPDATE...FROM (only modifies existing rows, never
    inserts). Called only on first_run when transforms modified rows that
    were just CREATEd from source.
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _check_display_name(name: str, check_registry: CheckRegistry) -> str:
    """Resolve check UUID to function name for log messages.

    Reads func_name from CheckContract — set once at registration time.
    No CLI imports — this reads data, not formatting.
    """
    try:
        contract = check_registry.get_contract(name)
        return contract.func_name or name
    except Exception:
        return name


def _drain_log(bundle: ReportBundle, callback: ReportCallback | None) -> None:
    """Drain all LogEntry items from a bundle through the callback.

    Called on the main thread between compute and write phases.
    Thread-safe: each bundle is owned by one thread during compute,
    and drained sequentially here.
    """
    if not callback:
        return
    for entry in bundle.log:
        callback.on_log(entry)


# ---------------------------------------------------------------------------
# Report execution — compute / write split
# ---------------------------------------------------------------------------

def _compute_report(
    report_config: dict,
    check_registry: CheckRegistry,
    pipeline_db: str,
    full_revalidation: bool = False,
) -> ReportBundle:
    """Read-only compute phase for one report. Safe to run in parallel.

    Opens a short-lived connection to pipeline.db for reads only (source
    table, validation_pass hashes, registry_types). All check and transform
    execution happens in pandas/memory. TRY_CAST gates use :memory: DuckDB.

    Progress messages are appended to bundle.log as LogEntry objects — never
    printed directly. The orchestrator drains them on the main thread.

    :return: ReportBundle with all data and flags ready for _write_report.
    """
    from proto_pipe.pipelines.flagging import (
        FlagRecord,
        compute_check_set_hash,
        compute_row_hash_sql,
    )
    from proto_pipe.io.db import (
        flag_id_for,
        get_validation_pass_hashes,
        get_current_check_set_hash,
        get_registry_types,
        table_exists,
    )
    from proto_pipe.checks.result import CheckResult as _CheckResult
    from proto_pipe.reports.transforms import _apply_transforms_with_gate

    report_name = report_config["name"]
    source_config = report_config["source"]
    source_table = source_config.get("table", "")
    pk_col = source_config.get("primary_key")
    alias_map = report_config.get("alias_map", [])
    target_table = report_config.get(
        "target_table",
        f"{source_table}_report" if source_table else report_name,
    )

    bundle = ReportBundle(
        report_name=report_name,
        status="computed",
        target_table=target_table,
        source_table=source_table,
        pk_col=pk_col,
    )

    conn = duckdb.connect(pipeline_db, read_only=True)
    _keep_conn_for_checks = False
    try:
        # 1. Compute check_set_hash
        check_entries = report_config.get("checks", [])
        current_hash = compute_check_set_hash(check_entries, check_registry, alias_map=alias_map)

        # 2. Detect check set changes
        prior_hash = get_current_check_set_hash(conn, report_name)
        if prior_hash and prior_hash != current_hash:
            bundle.log.append(LogEntry(
                level="warn",
                category="check_set_changed",
                report_name=report_name,
                message=(
                    f"Check set changed for '{report_name}'. "
                    f"Run 'vp validate --table {source_table} --full' to revalidate all records."
                ),
            ))
            full_revalidation = True

        # 3. Schema + row count
        try:
            schema_df = conn.execute(f'SELECT * FROM "{source_table}" LIMIT 0').df()
            all_columns = list(schema_df.columns)
            row_count = conn.execute(
                f'SELECT count(*) FROM "{source_table}"'
            ).fetchone()[0]
        except Exception as e:
            bundle.status = "error"
            bundle.error = str(e)
            return bundle

        if row_count == 0:
            bundle.status = "skipped"
            return bundle

        bundle.row_count = row_count

        # 4. Identify pending records via validation_pass
        comparable_cols = [
            c for c in all_columns
            if not c.startswith("_") and c != pk_col
        ]

        if pk_col and pk_col in all_columns:
            hash_expr = (
                compute_row_hash_sql(comparable_cols) if comparable_cols else "md5('')"
            )
            hash_df = conn.execute(f"""
                SELECT CAST("{pk_col}" AS VARCHAR) AS pk_value,
                       {hash_expr} AS row_hash
                FROM "{source_table}"
            """).df()
            incoming = dict(zip(hash_df["pk_value"], hash_df["row_hash"]))
        else:
            incoming = {str(i): "" for i in range(row_count)}

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
            bundle.status = "skipped"
            bundle.log.append(LogEntry(
                level="info",
                category="skip",
                report_name=report_name,
                message=f"'{report_name}' — no pending records",
            ))
            return bundle

        bundle.pending_pks = pending_pks
        bundle.first_run = not table_exists(conn, target_table)

        # 6. Read registry_types for TRY_CAST gate
        registry_types = get_registry_types(conn, all_columns)

        # 7. Determine execution path from contract flags
        all_names = report_config.get("resolved_checks", [])
        needs_pandas = False
        for _n in all_names:
            try:
                _c = check_registry.get_contract(_n)
                if _c.needs_dataframe or _c.is_legacy:
                    needs_pandas = True
                    break
            except ValueError:
                pass

        if needs_pandas:
            if pk_col and pk_col in all_columns:
                placeholders = ", ".join("?" * len(pending_pks))
                pending_df: pd.DataFrame | None = conn.execute(
                    f'SELECT * FROM "{source_table}"'
                    f' WHERE CAST("{pk_col}" AS VARCHAR) IN ({placeholders})',
                    list(pending_pks),
                ).df()
            else:
                pending_df = conn.execute(f'SELECT * FROM "{source_table}"').df()
            conn.close()
            conn = None
            context = {"df": pending_df}
        else:
            _keep_conn_for_checks = True
            needed_cols: set[str] = set()
            for n in all_names:
                try:
                    contract = check_registry.get_contract(n)
                except ValueError:
                    continue
                if contract.needs_series:
                    for col in contract.series_columns:
                        if col in all_columns:
                            needed_cols.add(col)

            col_cache: dict[str, "pd.Series"] = {}
            if needed_cols and pk_col and pending_pks:
                fetch_cols = list(needed_cols)
                col_list = ", ".join(f'"{c}"' for c in fetch_cols)
                placeholders = ", ".join("?" * len(pending_pks))
                cache_df = conn.execute(
                    f'SELECT {col_list} FROM "{source_table}"'
                    f' WHERE CAST("{pk_col}" AS VARCHAR) IN ({placeholders})',
                    list(pending_pks),
                ).df()
                for col in fetch_cols:
                    if col in cache_df.columns:
                        col_cache[col] = cache_df[col]

            pending_df = None
            context = {
                "conn": conn,
                "table": source_table,
                "pending_pks": pending_pks,
                "pk_col": pk_col,
                "all_columns": all_columns,
                "col_cache": col_cache,
            }

    finally:
        if conn is not None and not _keep_conn_for_checks:
            conn.close()

    bundle.log.append(LogEntry(
        level="info",
        category="pending",
        report_name=report_name,
        message=(
            f"{len(pending_pks)} pending record(s) of {row_count} total"
        ),
        count=len(pending_pks),
        total=row_count,
    ))

    try:
        # 8. Run checks
        check_names = []
        transform_names = []
        for n in all_names:
            try:
                kind = check_registry.get_kind(n)
            except ValueError:
                kind = "check"
            if kind == "check":
                check_names.append(n)
            elif kind == "transform":
                transform_names.append(n)

        results: dict[str, CheckOutcome] = {}
        accumulated_flags: list[FlagRecord] = []

        for check_name in check_names:
            try:
                result = check_registry.run(check_name, context)
                if isinstance(result, _CheckResult):
                    failing_mask = result.mask
                    failing_count = int(failing_mask.sum()) if failing_mask is not None else 0
                elif isinstance(result, pd.Series) and result.dtype == bool:
                    failing_mask = ~result
                    failing_count = int(failing_mask.sum())
                else:
                    failing_mask = None
                    failing_count = 0

                if failing_count > 0 and failing_mask is not None:
                    if pending_df is not None:
                        failing_df = pending_df[failing_mask]
                        pk_idx = (
                            list(failing_df.columns).index(pk_col)
                            if pk_col and pk_col in failing_df.columns
                            else None
                        )
                        for row in failing_df.itertuples(index=False, name=None):
                            pk_val = str(row[pk_idx]) if pk_idx is not None else None
                            accumulated_flags.append(FlagRecord(
                                id=flag_id_for(pk_val),
                                table_name=target_table,
                                report_name=report_name,
                                check_name=check_name,
                                pk_value=pk_val,
                                reason=f"Check '{check_name}' failed",
                            ))
                    else:
                        pk_list = list(pending_pks)
                        for i, failed in enumerate(failing_mask):
                            if failed and i < len(pk_list):
                                pk_val = pk_list[i]
                                accumulated_flags.append(FlagRecord(
                                    id=flag_id_for(pk_val),
                                    table_name=target_table,
                                    report_name=report_name,
                                    check_name=check_name,
                                    pk_value=pk_val,
                                    reason=f"Check '{check_name}' failed",
                                ))
                    bundle.log.append(LogEntry(
                        level="info",
                        category="check",
                        report_name=report_name,
                        message=(
                            f"'{_check_display_name(check_name, check_registry)}' — "
                            f"{failing_count} failure(s) → validation_block"
                        ),
                        check_name=check_name,
                        count=failing_count,
                    ))

                results[check_name] = CheckOutcome(
                    status="failed" if failing_count > 0 else "passed",
                    result=result if isinstance(result, _CheckResult) else None,
                )

            except Exception as e:
                bundle.log.append(LogEntry(
                    level="error",
                    category="check",
                    report_name=report_name,
                    message=f"'{_check_display_name(check_name, check_registry)}': {e}",
                    check_name=check_name,
                ))
                results[check_name] = CheckOutcome(status="error", error=str(e))

        # 9. Apply transforms
        modified_df = pending_df
        if transform_names:
            modified_df, transform_flags = _apply_transforms_with_gate(
                transform_names=transform_names,
                check_registry=check_registry,
                context=context,
                target_table=target_table,
                report_name=report_name,
                pk_col=pk_col,
                registry_types=registry_types,
                alias_map=alias_map,
                log=bundle.log,
            )
            accumulated_flags.extend(transform_flags)

    finally:
        if conn is not None:
            conn.close()

    # 10. Compute validation_pass entries in memory.
    all_passed = all(v.status == "passed" for v in results.values())

    # 10a. Add transform outcomes to results for display (after all_passed
    # so transforms don't affect watermark advancement — only checks matter).
    if transform_names:
        transform_error_names = {
            entry.check_name
            for entry in bundle.log
            if entry.category == "transform"
            and entry.level in ("error", "warn")
            and entry.check_name
        }
        for t_name in transform_names:
            if t_name in transform_error_names:
                results[t_name] = CheckOutcome(status="transform_error")
            else:
                results[t_name] = CheckOutcome(status="applied")

    flagged_pks = {fr.pk_value for fr in accumulated_flags if fr.pk_value}
    pass_entries = []
    for pk_str, row_hash in incoming.items():
        if pk_str in pending_pks:
            rec_status = "failed" if pk_str in flagged_pks else "passed"
            pass_entries.append({
                "pk_value": pk_str,
                "row_hash": row_hash,
                "check_set_hash": current_hash,
                "status": rec_status,
            })

    ts_col = source_config.get("timestamp_col") or "_ingested_at"

    bundle.results = results
    bundle.modified_df = modified_df
    bundle.flag_records = accumulated_flags
    bundle.pass_entries = pass_entries
    bundle.all_passed = all_passed
    bundle.timestamp_col = ts_col if ts_col in all_columns else None
    bundle.current_hash = current_hash

    return bundle


def _write_report(
    conn: duckdb.DuckDBPyConnection,
    bundle: ReportBundle,
    watermark_store: WatermarkStore,
    callback: ReportCallback | None = None,
) -> dict:
    """Sequential write phase. Persists one ReportBundle to DuckDB.

    Caller is responsible for the connection lifecycle. In run_all_reports
    one connection is shared across all bundles in a layer (sequential).
    In run_report a dedicated connection is opened and closed by the caller.

    Writes:
      - Report table (CREATE on first run, DELETE+INSERT on subsequent)
      - validation_block (check and transform failures)
      - validation_pass (per-record status)
      - Watermark (advanced only when all_passed)
    """
    from proto_pipe.pipelines.flagging import write_validation_flags
    from proto_pipe.io.db import bulk_upsert_validation_pass

    if bundle.status in ("skipped", "error"):
        return {
            "report": bundle.report_name,
            "status": bundle.status,
            "error": bundle.error,
        }

    source_table = bundle.source_table
    modified_df = bundle.modified_df
    pending_pks = bundle.pending_pks
    pk_col = bundle.pk_col
    target_table = bundle.target_table

    # Write report table
    if bundle.first_run:
        conn.execute(f'CREATE TABLE "{target_table}" AS SELECT * FROM "{source_table}"')
        if callback:
            callback.on_write_done(
                bundle.report_name, target_table, bundle.row_count, True
            )
        if modified_df is not None and not modified_df.empty:
            _update_report_table(conn, target_table, modified_df, pk_col)
    else:
        if pk_col and pk_col in modified_df.columns and not modified_df.empty:
            placeholders = ", ".join(["?"] * len(pending_pks))
            conn.execute(
                f'DELETE FROM "{target_table}" '
                f'WHERE CAST("{pk_col}" AS VARCHAR) IN ({placeholders})',
                list(pending_pks),
            )
            col_list = ", ".join(f'"{c}"' for c in modified_df.columns)
            conn.execute(
                f'INSERT INTO "{target_table}" ({col_list}) '
                f'SELECT {col_list} FROM modified_df'
            )

    # Write accumulated flags to validation_block
    if bundle.flag_records:
        write_validation_flags(conn, bundle.flag_records)

    # Bulk upsert validation_pass entries
    if bundle.pass_entries:
        bulk_upsert_validation_pass(
            conn, source_table, bundle.report_name, bundle.pass_entries
        )

    # Advance watermark via DuckDB MAX
    if bundle.all_passed and bundle.timestamp_col and bundle.source_table:
        max_ts_raw = conn.execute(
            f'SELECT MAX("{bundle.timestamp_col}") FROM "{bundle.source_table}"'
        ).fetchone()[0]
        if max_ts_raw is not None:
            if not isinstance(max_ts_raw, datetime):
                max_ts_raw = datetime.fromisoformat(str(max_ts_raw)).replace(tzinfo=timezone.utc)
            with watermark_lock:
                watermark_store.set(bundle.report_name, max_ts_raw)

    return {
        "report": bundle.report_name,
        "status": "completed",
        "results": bundle.results,
    }


def run_report(
    report_config: dict,
    check_registry: CheckRegistry,
    watermark_store: WatermarkStore,
    pipeline_db: str | None = None,
    full_revalidation: bool = False,
    callback: ReportCallback | None = None,
) -> dict:
    """Execute a single report — compute then write.

    pipeline_db is required. Used directly when a single report is run
    outside of run_all_reports (e.g. from tests or vp validate --table).
    """
    from proto_pipe.io.db import ensure_pipeline_tables
    _boot = duckdb.connect(pipeline_db)
    try:
        ensure_pipeline_tables(_boot)
    finally:
        _boot.close()

    bundle = _compute_report(
        report_config, check_registry, pipeline_db, full_revalidation
    )

    # Drain compute-phase log on main thread
    _drain_log(bundle, callback)

    conn = duckdb.connect(pipeline_db)
    try:
        return _write_report(conn, bundle, watermark_store, callback=callback)
    finally:
        conn.close()


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
    on_report_done: "callable | None" = None,
    report_names: "list[str] | None" = None,
    callback: ReportCallback | None = None,
) -> list[dict]:
    """Execute all reports in dependency order.

    report_names: optional list of report names to run. When provided only
    those reports execute. All reports are used for dependency resolution.

    on_report_done: optional callback fired after each report completes.
    Receives the result dict (same shape as the return list entries).
    Used by ValidateProgressReporter in cli/prompts.py for streaming output.
    Business logic is unchanged — the callback is a display concern only.

    callback: optional ReportCallback for structured progress output.
    Compute-phase LogEntry items are drained between phases; write-phase
    events fire directly.

    Reports are sorted into execution layers via _build_execution_layers.
    Reports in the same layer have no dependencies on each other.

    When parallel_reports=True (default):
      Phase 1 — compute: all reports in a layer run concurrently via
        ThreadPoolExecutor. Each _compute_report call opens its own
        short-lived read connection and does all check/transform work in
        memory. No file lock contention.
      Phase 2 — write: one shared connection is opened after all compute
        futures complete. _write_report is called sequentially per bundle.
        DuckDB's single-writer constraint is satisfied — one connection,
        one writer, no concurrent writes.

    When parallel_reports=False:
      run_report is called sequentially for each report (compute + write
      in one call). Useful for debugging or low-memory environments.

    Layers always execute one at a time regardless of parallel_reports.
    If a report finishes with status='error', direct dependents in later
    layers are skipped.

    Raises ValueError (from _build_execution_layers) if a dependency cycle
    is detected before any execution begins.
    """
    from proto_pipe.io.db import ensure_pipeline_tables

    all_reports = report_registry.all()
    target_to_name: dict[str, str] = {get_target_table(r): r["name"] for r in all_reports}
    reports = (
        [r for r in all_reports if r["name"] in set(report_names)]
        if report_names is not None
        else all_reports
    )
    layers = _build_execution_layers(reports)

    all_results: list[dict] = []
    failed_reports: set[str] = set()

    if pipeline_db:
        _bootstrap_conn = duckdb.connect(pipeline_db)
        try:
            ensure_pipeline_tables(_bootstrap_conn)
        finally:
            _bootstrap_conn.close()

    for layer in layers:
        runnable: list[dict] = []
        for r in layer:
            source_table = r.get("source", {}).get("table", "")
            upstream = target_to_name.get(source_table)
            if upstream and upstream in failed_reports:
                if callback:
                    callback.on_log(LogEntry(
                        level="info",
                        category="skip",
                        report_name=r["name"],
                        message=f"'{r['name']}' — upstream report '{upstream}' failed",
                    ))
                all_results.append({"report": r["name"], "status": "skipped"})
            else:
                runnable.append(r)

        if not runnable:
            continue

        if not parallel_reports or not pipeline_db:
            for r in runnable:
                result = run_report(
                    r, check_registry, watermark_store,
                    pipeline_db=pipeline_db,
                    full_revalidation=full_revalidation,
                    callback=callback,
                )
                all_results.append(result)
                if on_report_done:
                    on_report_done(result)
                if result.get("status") == "error":
                    failed_reports.add(r["name"])
        else:
            # Phase 1: parallel compute
            bundles: list[ReportBundle] = []
            with ThreadPoolExecutor() as executor:
                futures = {
                    executor.submit(
                        _compute_report,
                        r, check_registry,
                        pipeline_db, full_revalidation,
                    ): r["name"]
                    for r in runnable
                }
                for future in as_completed(futures):
                    try:
                        bundles.append(future.result())
                    except Exception as exc:
                        report_name = futures[future]
                        error_bundle = ReportBundle(
                            report_name=report_name,
                            status="error",
                            error=str(exc),
                        )
                        error_bundle.log.append(LogEntry(
                            level="error",
                            category="skip",
                            report_name=report_name,
                            message=f"'{report_name}' compute failed: {exc}",
                        ))
                        bundles.append(error_bundle)

            # Drain all compute-phase logs before write phase
            for bundle in bundles:
                _drain_log(bundle, callback)

            # Phase 2: sequential write
            conn = duckdb.connect(pipeline_db)
            try:
                for bundle in bundles:
                    result = _write_report(conn, bundle, watermark_store, callback=callback)
                    all_results.append(result)
                    if on_report_done:
                        on_report_done(result)
                    if result.get("status") == "error":
                        failed_reports.add(bundle.report_name)
            finally:
                conn.close()

    return all_results
