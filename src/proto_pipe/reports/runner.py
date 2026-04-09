"""Report runner — validation and deliverable execution.

Validation flow (vp validate):
  1. Compute check_set_hash for the report
  2. Compare against validation_pass to find pending records
  3. Run kind='check' functions → failures accumulated as FlagRecord objects
  4. Run kind='transform' functions → TRY_CAST gate via :memory: DuckDB → bad rows accumulated
  5. Write report table, validation_block, validation_pass (sequential, one connection)
  6. Advance watermark only when all checks passed

Parallel execution model:
  _compute_report  — read-only, parallelisable. Opens short-lived read connection,
                     runs all checks/transforms in memory, returns ReportBundle.
  _write_report    — sequential, shared connection. Persists ReportBundle to DuckDB.
  run_all_reports  — Phase 1: parallel _compute_report per layer via ThreadPoolExecutor.
                     Phase 2: sequential _write_report with one shared connection.

Report table lifecycle:
  - Named in report config as target_table
  - First vp validate: CREATE TABLE AS SELECT from source, then UPDATE with transform results
  - Subsequent runs: DELETE pending PKs, INSERT modified_df (transforms already applied)
  - Source table is never modified by this layer
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import timezone, datetime
from pathlib import Path

import duckdb
import pandas as pd

from proto_pipe.checks.runner import run_checks
from proto_pipe.checks.result import CheckOutcome
from proto_pipe.io.registry import resolve_filename, write_xlsx_sheet, write_csv
from proto_pipe.pipelines.watermark import WatermarkStore, _watermark_lock
from proto_pipe.checks.registry import CheckRegistry, ReportRegistry, CheckParamInspector
from proto_pipe.constants import DUCKDB_TYPE_MAP
from proto_pipe.pipelines.query import _log_run, init_report_runs_table


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


# ---------------------------------------------------------------------------
# Dependency inference helpers
# ---------------------------------------------------------------------------

def _get_target_table(report_config: dict) -> str:
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
    target_table is resolved via _get_target_table: explicit config value first,
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

    # Map each report's output table back to the report name that produces it.
    target_to_name: dict[str, str] = {_get_target_table(r): r["name"] for r in reports}

    # Build directed edges: dependencies[B] = {A, ...} means B depends on A.
    dependencies: dict[str, set[str]] = {r["name"]: set() for r in reports}
    dependents: dict[str, set[str]] = {r["name"]: set() for r in reports}

    for r in reports:
        source_table = r.get("source", {}).get("table", "")
        upstream_name = target_to_name.get(source_table)
        if upstream_name and upstream_name != r["name"]:
            dependencies[r["name"]].add(upstream_name)
            dependents[upstream_name].add(r["name"])

    # Kahn's algorithm — BFS topological sort into layers.
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

    # Cycle detection — any report still with in_degree > 0 is part of a cycle.
    scheduled = sum(len(layer) for layer in layers)
    if scheduled < len(reports):
        cycled = sorted(name for name, deg in in_degree.items() if deg > 0)
        raise ValueError(
            f"Dependency cycle detected among reports: {', '.join(cycled)}. "
            f"Check source.table and target_table config for circular references."
        )

    return layers


def _resolve_display_name(check_name: str, check_registry) -> str:
    """Thin wrapper — delegates to prompts.display_name (the canonical location).

    display_name is a display concern and lives in cli/prompts.py per the
    module responsibility rule. This wrapper avoids a top-level import of
    prompts in reports/runner.py (which would create a circular dependency).
    Lazy import inside the function breaks the cycle.
    """
    from proto_pipe.cli.prompts import display_name
    return display_name(check_name, check_registry)


# ---------------------------------------------------------------------------
# Transform application — with TRY_CAST gate
# ---------------------------------------------------------------------------

def _apply_transforms_with_gate(
    transform_names: list[str],
    check_registry: CheckRegistry,
    context: dict,
    target_table: str,
    report_name: str,
    pk_col: str | None,
    registry_types: dict[str, str],
    alias_map: list[dict] | None = None,
) -> tuple[pd.DataFrame, list]:
    """Apply transforms with a TRY_CAST gate. Returns (modified_df, flag_records).

    No DB writes — all results are returned in-memory for _write_report to
    persist. This allows the compute phase to run in parallel across reports
    without contending on the DuckDB file lock.

    Accepts either a pandas context {"df": pending_df} or a DuckDB context
    {"conn": conn, "table": ..., "pending_pks": ..., "pk_col": ..., "all_columns": ...}.
    For the DuckDB path, pending rows are loaded lazily only when a transform
    needs them (Series/DataFrame transforms require pandas; scalar transforms
    stay in DuckDB via UDF).

    Uses CheckRegistry.get_contract() to read inspection flags set at
    registration time — never re-runs CheckParamInspector at call time.

    TRY_CAST uses duckdb.connect(':memory:') — no file connection needed.

    :return: (modified_df, flag_records)
    """
    from proto_pipe.pipelines.flagging import FlagRecord
    from proto_pipe.io.db import flag_id_for

    accumulated_flags: list[FlagRecord] = []

    # Resolve working DataFrame for transforms that need pandas.
    # On pandas path: already have df. On DuckDB path: load lazily on first need.
    _df: pd.DataFrame | None = context.get("df")

    def _get_df() -> pd.DataFrame:
        """Load pending rows from DuckDB on first pandas-requiring transform."""
        nonlocal _df
        if _df is not None:
            return _df
        conn = context["conn"]
        table = context["table"]
        pending_pks = context["pending_pks"]
        _pk_col = context["pk_col"]
        if _pk_col and pending_pks:
            placeholders = ", ".join("?" * len(pending_pks))
            _df = conn.execute(
                f'SELECT * FROM "{table}"'
                f' WHERE CAST("{_pk_col}" AS VARCHAR) IN ({placeholders})',
                list(pending_pks),
            ).df()
        else:
            _df = conn.execute(f'SELECT * FROM "{table}"').df()
        return _df

    for name in transform_names:
        try:
            func = check_registry.get(name)
            # Use stored contract flags — never re-run CheckParamInspector.
            # is_scalar: no Series/DataFrame params → DuckDB UDF path.
            # needs_dataframe or needs_series: requires pandas execution.
            contract = check_registry.get_contract(name)
            is_scalar_transform = contract.is_scalar

            if is_scalar_transform:
                df = _get_df()
                try:
                    df = _apply_scalar_transform_duckdb(
                        name, func, df, registry_types,
                        alias_map=alias_map,
                        check_name=_resolve_display_name(name, check_registry),
                    )
                    _df = df  # keep _df in sync after mutation
                    continue
                except Exception as udf_exc:
                    print(
                        f"  [transform] '{_resolve_display_name(name, check_registry)}' DuckDB UDF failed ({udf_exc}) "
                        f"— falling back to pandas"
                    )

            # Pandas execution path — pass context directly to wrappers.
            # Series/scalar wrappers handle both pandas and DuckDB context.
            # DataFrame wrappers always receive pandas context (needs_dataframe
            # forces the pandas path in _compute_report).
            df = _get_df()
            result = check_registry.run(name, {"df": df})

            if isinstance(result, pd.DataFrame):
                modified_df = result.copy()
            elif isinstance(result, pd.Series):
                col_name = result.name
                # If result.name is missing or not in the table, fall back to
                # the alias_map _output entry. Use col_backed_map from the
                # CheckContract — extracted once at registration time per the
                # inspect-once principle. Never re-inspect the partial at runtime.
                if not col_name or col_name not in df.columns:
                    _contract = check_registry.get_contract(name)
                    col_name = _resolve_output_col(
                        alias_map or [],
                        _contract.col_backed_params if _contract else {},
                        check_name=_resolve_display_name(name, check_registry),
                    )
                if not col_name or col_name not in df.columns:
                    print(
                        f"  [transform-warn] '{_resolve_display_name(name, check_registry)}' returned Series "
                        f"with name={result.name!r} — not found in table and no _output entry, skipped"
                    )
                    continue
                modified_df = df.copy()
                modified_df[col_name] = result.values
            else:
                print(
                    f"  [transform-warn] '{_resolve_display_name(name, check_registry)}' returned {type(result).__name__} "
                    f"(expected DataFrame or Series) — skipped"
                )
                continue

            # TRY_CAST gate — in-memory DuckDB connection, no file access needed
            user_cols = [c for c in modified_df.columns if not c.startswith("_")]
            affected_types = {
                col: rt for col, rt in registry_types.items() if col in user_cols
            }

            blocked_pks: set[str] = set()
            if affected_types and pk_col and pk_col in modified_df.columns:
                mem_conn = duckdb.connect(":memory:")
                try:
                    for col, declared_type in affected_types.items():
                        if col not in modified_df.columns:
                            continue
                        base_type = (
                            declared_type.split("|")[0].upper()
                            if "|" in declared_type
                            else declared_type.upper()
                        )
                        try:
                            failing = mem_conn.execute(f"""
                                SELECT CAST("{pk_col}" AS VARCHAR) AS pk_val,
                                       CAST("{col}" AS VARCHAR) AS raw_value
                                FROM modified_df
                                WHERE TRY_CAST("{col}" AS {base_type}) IS NULL
                                AND "{col}" IS NOT NULL
                            """).df()
                        except Exception:
                            continue

                        for row in failing.itertuples(index=False, name=None):
                            pk_str = str(row[0])
                            blocked_pks.add(pk_str)
                            accumulated_flags.append(FlagRecord(
                                id=flag_id_for(pk_str),
                                table_name=target_table,
                                report_name=report_name,
                                check_name=name,
                                pk_value=pk_str,
                                bad_columns=col,
                                reason=(
                                    f"[{col}] transform '{_resolve_display_name(name, check_registry)}' produced "
                                    f"'{row[1]}' which cannot be cast to {base_type}"
                                )[:500],
                            ))
                finally:
                    mem_conn.close()

                if blocked_pks:
                    print(
                        f"  [transform-warn] '{_resolve_display_name(name, check_registry)}' produced {len(blocked_pks)} "
                        f"row(s) that failed TRY_CAST → validation_block"
                    )
                    modified_df = modified_df[
                        ~modified_df[pk_col].astype(str).isin(blocked_pks)
                    ].copy()

            if modified_df.empty:
                continue

            df = modified_df  # carry forward for subsequent transforms
            _df = df          # keep _df in sync so _get_df() returns updated df

        except Exception as e:
            print(f"  [transform-fail] '{_resolve_display_name(name, check_registry)}': {e} — skipped, table unchanged")

    print(
        f"  [transform] {len(transform_names)} transform(s) applied to '{target_table}'"
    )
    return _get_df(), accumulated_flags


def _annotation_to_duckdb(ann) -> type:
    """Map a Python return annotation to a Python type for DuckDB UDF registration.

    DuckDB cannot infer return type from *args wrapper functions — it must be
    supplied explicitly. DuckDB accepts Python native types (float, int, str, bool)
    directly in create_function(), so no duckdb.typing import is needed.
    Falls back to float (DOUBLE) for unrecognised or missing annotations.
    """
    import inspect
    if ann is inspect.Parameter.empty:
        return float
    s = str(ann).lower()
    if "bool" in s:   return bool
    if "float" in s:  return float
    if "int" in s:    return int
    if "str" in s:    return str
    return float


def _resolve_output_col(
    alias_map: list[dict],
    col_backed_params: dict[str, str],
    check_name: str | None = None,
) -> str | None:
    """Resolve the _output write-back column for a given expanded check run.

    Matches the baked-in column values (col_backed_params) against the alias_map
    to determine the run index, then returns the corresponding _output column.

    check_name: base function name (from _display_name) used to scope _output
    entries to this specific transform. Without scoping, all transforms' _output
    entries are merged and the wrong column may be selected.

    col_backed_params: {param_name: column_name} for all column-backed params
    in this registered expanded check (derived from the partial's keywords).

    Returns None when no _output entries exist in alias_map (legacy transforms
    that write back to the input column fall through to the caller's fallback).
    """
    # Filter _output entries by check_name if present — scopes to this transform.
    # Falls back to all _output entries for legacy alias_maps without "check" field.
    output_entries = [
        e for e in alias_map
        if e["param"] == "_output"
        and (check_name is None or e.get("check") == check_name or "check" not in e)
    ]
    output_cols = [e["column"] for e in output_entries]
    if not output_cols:
        return None

    # Find run index by matching any baked column value against its alias_map entries
    for param_name, baked_col in col_backed_params.items():
        param_entries = [
            e["column"] for e in alias_map
            if e["param"] == param_name and e["param"] != "_output"
        ]
        if baked_col in param_entries:
            run_index = param_entries.index(baked_col)
            if run_index < len(output_cols):
                return output_cols[run_index]

    return None


def _apply_scalar_transform_duckdb(
    name: str,
    func,
    df: pd.DataFrame,
    registry_types: dict[str, str],
    alias_map: list[dict] | None = None,
    check_name: str | None = None,
) -> pd.DataFrame:
    """Apply a scalar transform via DuckDB Python UDF. Returns modified DataFrame.

    Uses an in-memory DuckDB connection — no file access, safe for parallel
    execution in the compute phase. The UDF UPDATE runs against an in-memory
    copy of df; the result is read back and returned.

    Raises on failure so the caller falls back to pandas.
    """
    import inspect

    df_cols = set(df.columns)

    baked_keywords: dict = {}
    if hasattr(func, "keywords"):
        baked_keywords = dict(func.keywords)

    original = func
    while hasattr(original, "func"):
        original = original.func
    sig = inspect.signature(original)
    all_params = [p.name for p in sig.parameters.values()]

    if not all_params:
        raise ValueError(f"Scalar transform '{check_name or name}' has no parameters")

    col_backed: dict[str, str] = {}
    for param_name in all_params:
        baked_val = baked_keywords.get(param_name)
        if isinstance(baked_val, str) and baked_val in df_cols:
            col_backed[param_name] = baked_val

    if not col_backed:
        raise ValueError(
            f"Scalar transform '{check_name or name}' has no column-backed params in alias_map "
            f"and no first-param column to operate on."
        )

    output_col = _resolve_output_col(alias_map or [], col_backed, check_name=check_name) if alias_map else None
    if output_col is None:
        first_col_param = next((p for p in all_params if p in col_backed), None)
        if first_col_param is None:
            raise ValueError(f"Cannot determine output column for transform '{check_name or name}'")
        output_col = col_backed[first_col_param]

    if output_col not in df_cols:
        raise ValueError(
            f"Output column '{output_col}' for transform '{check_name or name}' not in DataFrame"
        )

    col_param_names = [p for p in all_params if p in col_backed]
    sql_col_args = ", ".join(f'"{col_backed[p]}"' for p in col_param_names)
    constant_kwargs = {k: v for k, v in baked_keywords.items() if k not in col_backed}

    _arg_names = [f"_a{i}" for i in range(len(col_param_names))]
    _call_kwargs = ", ".join(
        f"'{col_param_names[i]}': _a{i}" for i in range(len(col_param_names))
    )
    _exec_ns: dict = {"original": original, "constant_kwargs": constant_kwargs}
    exec(
        f"def _wrapper({', '.join(_arg_names)}): "
        f"return original(**{{{_call_kwargs}}}, **constant_kwargs)",
        _exec_ns,
    )
    udf_wrapper = _exec_ns["_wrapper"]

    _tmp_table = "_scalar_udf_tmp"
    udf_name = f"_udf_{name}"
    mem_conn = duckdb.connect(":memory:")
    try:
        mem_conn.execute(f'CREATE TABLE "{_tmp_table}" AS SELECT * FROM df')
        param_ddb_types = [
            DUCKDB_TYPE_MAP.get(
                registry_types.get(col_backed[p], "DOUBLE").upper().split("|")[0].strip(),
                DUCKDB_TYPE_MAP["DOUBLE"],
            )
            for p in col_param_names
        ]
        return_ddb_type = _annotation_to_duckdb(sig.return_annotation)
        mem_conn.create_function(udf_name, udf_wrapper, param_ddb_types, return_ddb_type)
        mem_conn.execute(
            f'UPDATE "{_tmp_table}" SET "{output_col}" = {udf_name}({sql_col_args})'
        )
        return mem_conn.execute(f'SELECT * FROM "{_tmp_table}"').df()
    finally:
        try:
            mem_conn.remove_function(udf_name)
        except Exception:
            pass
        mem_conn.close()


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


# ---------------------------------------------------------------------------
# Report execution — compute / write split
#
# _compute_report: reads source data, runs checks and transforms in memory.
#   Opens its own short-lived read connection per report. Safe to run in
#   parallel across reports in the same execution layer.
#
# _write_report: takes a shared write connection and a ReportBundle, persists
#   the report table, validation_block, and validation_pass. Always sequential.
#
# run_report: calls both in sequence. Used by single-report callers.
# run_all_reports: runs _compute_report in parallel per layer, then
#   _write_report sequentially with one shared connection per layer.
# ---------------------------------------------------------------------------

def _compute_report(
    report_config: dict,
    check_registry: CheckRegistry,
    watermark_store: WatermarkStore,
    pipeline_db: str,
    full_revalidation: bool = False,
) -> ReportBundle:
    """Read-only compute phase for one report. Safe to run in parallel.

    Opens a short-lived connection to pipeline.db for reads only (source
    table, validation_pass hashes, registry_types). All check and transform
    execution happens in pandas/memory. TRY_CAST gates use :memory: DuckDB.

    :return: ReportBundle with all data and flags ready for _write_report.
    """
    from proto_pipe.pipelines.flagging import (
        FlagRecord,
        write_validation_flags,
        compute_check_set_hash,
        compute_row_hash,
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

    report_name = report_config["name"]
    source_config = report_config["source"]
    source_table = source_config.get("table", "")
    pk_col = source_config.get("primary_key")
    alias_map = report_config.get("alias_map", [])
    target_table = report_config.get(
        "target_table",
        f"{source_table}_report" if source_table else report_name,
    )

    # read_only=True allows multiple concurrent threads to open this file
    # simultaneously without acquiring the exclusive write lock. This is what
    # makes Phase 1 parallel execution safe — all _compute_report calls are
    # reading, never writing.
    conn = duckdb.connect(pipeline_db, read_only=True)
    # Flag: True only when the DuckDB-native path keeps conn open for check
    # execution. Initialized before the try block so the finally clause can
    # always read it — even when an early return fires inside the try.
    _keep_conn_for_checks = False
    try:
        # 1. Compute check_set_hash
        check_entries = report_config.get("checks", [])
        current_hash = compute_check_set_hash(check_entries, check_registry, alias_map=alias_map)

        # 2. Detect check set changes
        prior_hash = get_current_check_set_hash(conn, report_name)
        if prior_hash and prior_hash != current_hash:
            print(
                f"  [warn] Check set changed for '{report_name}'. "
                f"Run 'vp validate --table {source_table} --full' to revalidate all records."
            )
            full_revalidation = True

        # 3. Schema + row count — no full pandas load yet.
        #    LIMIT 0: column names without transferring any rows.
        #    count(*): scalar — confirms table is non-empty.
        try:
            schema_df = conn.execute(f'SELECT * FROM "{source_table}" LIMIT 0').df()
            all_columns = list(schema_df.columns)
            row_count = conn.execute(
                f'SELECT count(*) FROM "{source_table}"'
            ).fetchone()[0]
        except Exception as e:
            return ReportBundle(report_name=report_name, status="error", error=str(e))

        if row_count == 0:
            return ReportBundle(report_name=report_name, status="skipped")

        # 4. Identify pending records via validation_pass.
        #    Hashes computed directly against source table — 1 SQL query
        #    replaces N Python hash calls (same invariant as ingest.py).
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
            print(f"  [skip] '{report_name}' — no pending records")
            return ReportBundle(report_name=report_name, status="skipped")

        first_run = not table_exists(conn, target_table)

        # 6. Read registry_types for TRY_CAST gate (read-only)
        registry_types = get_registry_types(conn, all_columns)

        # 7. Determine execution path from contract flags — inspect once at
        #    registration, read here. If any check or transform needs a full
        #    pandas DataFrame, or uses the legacy context: dict convention,
        #    load pending rows to pandas and close the connection early.
        #    Otherwise keep conn open and pass a DuckDB context to wrappers —
        #    they extract only the columns they need, per check.
        all_names = report_config.get("resolved_checks", [])
        needs_pandas = False
        for _n in all_names:
            try:
                _c = check_registry.get_contract(_n)
                if _c.needs_dataframe or _c.is_legacy:
                    needs_pandas = True
                    break
            except ValueError:
                pass  # unregistered — will produce CheckOutcome(status="error") in execution loop

        if needs_pandas:
            # Pandas path: load only pending rows, then close connection.
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
            # DuckDB-native path: keep conn open during check execution.
            # Pre-fetch all columns needed by Series checks in one query per
            # report — replaces N individual SELECT queries (one per check param)
            # with a single bulk fetch. alias_map ties params to columns in
            # this report's source table only — no cross-table bleed.
            _keep_conn_for_checks = True

            # Collect all Series param column names from contracts — stored once
            # at registration time, read here. No re-inspection of wrapped funcs.
            needed_cols: set[str] = set()
            for n in all_names:
                try:
                    contract = check_registry.get_contract(n)
                except ValueError:
                    continue  # unregistered — skip series column collection
                if contract.needs_series:
                    for col in contract.series_columns:
                        if col in all_columns:
                            needed_cols.add(col)

            # Single bulk fetch — all needed columns + pk for pending rows
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
        # Close conn on early returns (error/skip/empty) and on the pandas path
        # (pandas path already set conn=None, so this is a no-op for it).
        # DuckDB-native path: _keep_conn_for_checks=True → leave open for checks.
        if conn is not None and not _keep_conn_for_checks:
            conn.close()

    print(
        f"  [{report_name}] {len(pending_pks)} pending record(s) of "
        f"{row_count} total"
    )

    try:
        # 8. Run checks — wrappers handle context type transparently.
        # Unregistered names default to kind="check" so they reach the
        # execution loop's except clause and produce CheckOutcome(status="error").
        check_names = []
        transform_names = []
        for n in all_names:
            try:
                kind = check_registry.get_kind(n)
            except ValueError:
                kind = "check"  # unregistered → route to execution loop error handler
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
                    # Build failing rows for flag accumulation.
                    # Pandas path: filter pending_df by mask.
                    # DuckDB path: load failing PKs from the result Series index.
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
                        # DuckDB path: the result Series index corresponds to
                        # the order returned by the DuckDB query (pk IN filter).
                        # Reconstruct failing PKs from pending_pks order.
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
                    print(
                        f"  [check] '{_resolve_display_name(check_name, check_registry)}' — "
                        f"{failing_count} failure(s) → validation_block"
                    )

                results[check_name] = CheckOutcome(
                    status="failed" if failing_count > 0 else "passed",
                    result=result if isinstance(result, _CheckResult) else None,
                )

            except Exception as e:
                print(f"  [check-fail] '{_resolve_display_name(check_name, check_registry)}': {e}")
                results[check_name] = CheckOutcome(status="error", error=str(e))

        # 9. Apply transforms — context carries either {"df": ...} or DuckDB
        #    context. _apply_transforms_with_gate resolves df lazily if needed.
        modified_df = pending_df  # None on DuckDB path until transforms load it
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
            )
            accumulated_flags.extend(transform_flags)

    finally:
        # Close DuckDB-native connection now that checks are complete.
        if conn is not None:
            conn.close()

    # 10. Compute validation_pass entries in memory.
    all_passed = all(v.status == "passed" for v in results.values())
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

    return ReportBundle(
        report_name=report_name,
        status="computed",
        results=results,
        target_table=target_table,
        source_table=source_table,
        modified_df=modified_df,
        first_run=first_run,
        pending_pks=pending_pks,
        pk_col=pk_col,
        flag_records=accumulated_flags,
        pass_entries=pass_entries,
        all_passed=all_passed,
        timestamp_col=ts_col if ts_col in all_columns else None,
        current_hash=current_hash,
    )


def _write_report(
    conn: duckdb.DuckDBPyConnection,
    bundle: ReportBundle,
    watermark_store: WatermarkStore,
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
    from proto_pipe.io.db import bulk_upsert_validation_pass, table_exists

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
        # CREATE directly from source table in DuckDB — no Python DataFrame
        # intermediate. source_df no longer stored in bundle.
        conn.execute(f'CREATE TABLE "{target_table}" AS SELECT * FROM "{source_table}"')
        print(f"  [ok] Created report table '{target_table}' ({bundle.row_count} rows)")
        if modified_df is not None and not modified_df.empty:
            _update_report_table(conn, target_table, modified_df, pk_col)
    else:
        # Subsequent run — DELETE pending PKs then INSERT post-transform records.
        # Transforms are already applied to modified_df in _compute_report, so
        # no separate UPDATE step is needed here.
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

    # Bulk upsert validation_pass entries — one operation replaces N individual
    # upserts. On first run with 1M records this is the dominant cost reduction:
    # 1M INSERT ... ON CONFLICT statements → 1 DataFrame scan + INSERT.
    if bundle.pass_entries:
        bulk_upsert_validation_pass(
            conn, source_table, bundle.report_name, bundle.pass_entries
        )

    # Advance watermark via DuckDB MAX — no pandas source_df needed.
    if bundle.all_passed and bundle.timestamp_col and bundle.source_table:
        max_ts_raw = conn.execute(
            f'SELECT MAX("{bundle.timestamp_col}") FROM "{bundle.source_table}"'
        ).fetchone()[0]
        if max_ts_raw is not None:
            if not isinstance(max_ts_raw, datetime):
                max_ts_raw = datetime.fromisoformat(str(max_ts_raw)).replace(tzinfo=timezone.utc)
            with _watermark_lock:
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
) -> dict:
    """Execute a single report — compute then write.

    pipeline_db is required. Used directly when a single report is run
    outside of run_all_reports (e.g. from tests or vp validate --table).
    """
    report_name = report_config["name"]

    # Bootstrap pipeline tables before _compute_report reads validation_pass.
    # run_report is callable directly (e.g. from tests, vp validate --table)
    # without going through run_all_reports, so it cannot rely on the
    # run_all_reports bootstrap connection. Safe to call multiple times —
    # all CREATE statements use IF NOT EXISTS.
    from proto_pipe.io.db import ensure_pipeline_tables
    _boot = duckdb.connect(pipeline_db)
    try:
        ensure_pipeline_tables(_boot)
    finally:
        _boot.close()

    bundle = _compute_report(
        report_config, check_registry, watermark_store, pipeline_db, full_revalidation
    )

    conn = duckdb.connect(pipeline_db)
    try:
        return _write_report(conn, bundle, watermark_store)
    finally:
        conn.close()

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
    on_report_done: "callable | None" = None,
    report_names: "list[str] | None" = None,
) -> list[dict]:
    """Execute all reports in dependency order.

    report_names: optional list of report names to run. When provided only
    those reports execute. All reports are used for dependency resolution.

    on_report_done: optional callback fired after each report completes.
    Receives the result dict (same shape as the return list entries).
    Used by ValidateProgressReporter in cli/prompts.py for streaming output.
    Business logic is unchanged — the callback is a display concern only.

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
    # Build dependency map from ALL reports — needed for upstream failure propagation
    # even when only a subset is being executed.
    target_to_name: dict[str, str] = {_get_target_table(r): r["name"] for r in all_reports}
    reports = (
        [r for r in all_reports if r["name"] in set(report_names)]
        if report_names is not None
        else all_reports
    )
    layers = _build_execution_layers(reports)

    all_results: list[dict] = []
    failed_reports: set[str] = set()

    # Guarantee pipeline tables exist before any compute phase reads them.
    # Done once here rather than per-report to avoid redundant CREATE IF NOT EXISTS
    # calls and to ensure reads in _compute_report never race a missing table.
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
                print(f"  [skip] '{r['name']}' — upstream report '{upstream}' failed")
                all_results.append({"report": r["name"], "status": "skipped"})
            else:
                runnable.append(r)

        if not runnable:
            continue

        if not parallel_reports or not pipeline_db:
            # Sequential path — each run_report opens and closes its own connection
            for r in runnable:
                result = run_report(
                    r, check_registry, watermark_store,
                    pipeline_db=pipeline_db,
                    full_revalidation=full_revalidation,
                )
                all_results.append(result)
                if on_report_done:
                    on_report_done(result)
                if result.get("status") == "error":
                    failed_reports.add(r["name"])
        else:
            # Phase 1: parallel compute — all read-only, no file lock contention
            bundles: list[ReportBundle] = []
            with ThreadPoolExecutor() as executor:
                futures = {
                    executor.submit(
                        _compute_report,
                        r, check_registry, watermark_store,
                        pipeline_db, full_revalidation,
                    ): r["name"]
                    for r in runnable
                }
                for future in as_completed(futures):
                    try:
                        bundles.append(future.result())
                    except Exception as exc:
                        report_name = futures[future]
                        print(f"  [error] '{report_name}' compute failed: {exc}")
                        bundles.append(ReportBundle(
                            report_name=report_name,
                            status="error",
                            error=str(exc),
                        ))

            # Phase 2: sequential write — one shared connection, no contention
            conn = duckdb.connect(pipeline_db)
            try:
                for bundle in bundles:
                    result = _write_report(conn, bundle, watermark_store)
                    all_results.append(result)
                    if on_report_done:
                        on_report_done(result)
                    if result.get("status") == "error":
                        failed_reports.add(bundle.report_name)
            finally:
                conn.close()

    return all_results
