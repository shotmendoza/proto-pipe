"""Transform application — TRY_CAST gate and DuckDB UDF path.

Moved from reports/runner.py per REFACTOR_PLAN.md. Contains:
  - _apply_transforms_with_gate: runs transforms, accumulates flags
  - _apply_scalar_transform_duckdb: DuckDB UDF execution path
  - _resolve_output_col: alias_map _output column resolution
  - _annotation_to_duckdb: Python annotation → DuckDB type mapping

No print() — structured LogEntry messages appended to the log list
parameter. The caller (typically _compute_report) passes its
ReportBundle.log list; the orchestrator drains it through
ReportCallback.on_log() on the main thread.

No CLI imports — reports/ never imports click, rich, or questionary.
"""
from __future__ import annotations

import duckdb
import pandas as pd

from proto_pipe.checks.registry import CheckRegistry
from proto_pipe.constants import DUCKDB_TYPE_MAP
from proto_pipe.reports.callbacks import LogEntry


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
    log: list | None = None,
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

    :param log: list to append LogEntry messages to (caller's ReportBundle.log)
    :return: (modified_df, flag_records)
    """
    from proto_pipe.pipelines.flagging import FlagRecord
    from proto_pipe.io.db import flag_id_for

    _log = log if log is not None else []
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

    def _check_display_name(name: str) -> str:
        """Resolve check UUID to function name for log messages."""
        try:
            contract = check_registry.get_contract(name)
            return contract.func_name or name
        except Exception:
            return name

    for name in transform_names:
        try:
            func = check_registry.get(name)
            contract = check_registry.get_contract(name)
            is_scalar_transform = contract.is_scalar
            disp_name = _check_display_name(name)

            if is_scalar_transform:
                df = _get_df()
                try:
                    df = _apply_scalar_transform_duckdb(
                        name, func, df, registry_types,
                        alias_map=alias_map,
                        check_name=disp_name,
                    )
                    _df = df  # keep _df in sync after mutation
                    continue
                except Exception as udf_exc:
                    _log.append(LogEntry(
                        level="warn",
                        category="transform",
                        report_name=report_name,
                        message=(
                            f"'{disp_name}' DuckDB UDF failed ({udf_exc}) "
                            f"— falling back to pandas"
                        ),
                        check_name=name,
                    ))

            # Pandas execution path
            df = _get_df()
            result = check_registry.run(name, {"df": df})

            if isinstance(result, pd.DataFrame):
                modified_df = result.copy()
            elif isinstance(result, pd.Series):
                col_name = result.name
                if not col_name or col_name not in df.columns:
                    _contract = check_registry.get_contract(name)
                    col_name = _resolve_output_col(
                        alias_map or [],
                        _contract.col_backed_params if _contract else {},
                        check_name=disp_name,
                    )
                if not col_name or col_name not in df.columns:
                    _log.append(LogEntry(
                        level="warn",
                        category="transform",
                        report_name=report_name,
                        message=(
                            f"'{disp_name}' returned Series with name={result.name!r} "
                            f"— not found in table and no _output entry, skipped"
                        ),
                        check_name=name,
                    ))
                    continue
                modified_df = df.copy()
                modified_df[col_name] = result.values
            else:
                _log.append(LogEntry(
                    level="warn",
                    category="transform",
                    report_name=report_name,
                    message=(
                        f"'{disp_name}' returned {type(result).__name__} "
                        f"(expected DataFrame or Series) — skipped"
                    ),
                    check_name=name,
                ))
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
                                    f"[{col}] transform '{disp_name}' produced "
                                    f"'{row[1]}' which cannot be cast to {base_type}"
                                )[:500],
                            ))
                finally:
                    mem_conn.close()

                if blocked_pks:
                    _log.append(LogEntry(
                        level="warn",
                        category="transform",
                        report_name=report_name,
                        message=(
                            f"'{disp_name}' produced {len(blocked_pks)} "
                            f"row(s) that failed TRY_CAST → validation_block"
                        ),
                        check_name=name,
                        count=len(blocked_pks),
                    ))
                    modified_df = modified_df[
                        ~modified_df[pk_col].astype(str).isin(blocked_pks)
                    ].copy()

            if modified_df.empty:
                continue

            df = modified_df  # carry forward for subsequent transforms
            _df = df          # keep _df in sync so _get_df() returns updated df

        except Exception as e:
            _log.append(LogEntry(
                level="error",
                category="transform",
                report_name=report_name,
                message=f"'{_check_display_name(name)}': {e} — skipped, table unchanged",
                check_name=name,
            ))

    _log.append(LogEntry(
        level="info",
        category="transform",
        report_name=report_name,
        message=(
            f"{len(transform_names)} transform(s) applied to '{target_table}'"
        ),
        count=len(transform_names),
    ))
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

    check_name: base function name (from contract.func_name) used to scope
    _output entries to this specific transform.

    col_backed_params: {param_name: column_name} for all column-backed params
    in this registered expanded check (derived from the partial's keywords).

    Returns None when no _output entries exist in alias_map.
    """
    output_entries = [
        e for e in alias_map
        if e["param"] == "_output"
        and (check_name is None or e.get("check") == check_name or "check" not in e)
    ]
    output_cols = [e["column"] for e in output_entries]
    if not output_cols:
        return None

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
    execution in the compute phase.

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
            pass  # remove_function can fail if connection is closing — harmless
        mem_conn.close()
