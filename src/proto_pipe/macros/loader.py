"""Macro loading — SQL file loading and Python UDF registration.

load_sql_macros: reads .sql files, parses signatures, registers in
    MacroRegistry, executes CREATE MACRO in DuckDB.
register_python_macros: iterates MacroRegistry for source='python'
    contracts, calls conn.create_function for each.
load_all_macros: orchestrates the full macro loading sequence for a
    connection — called by vp deliver and vp run-all.
smoke_test_macros: parse-only validation for vp init db — no CREATE
    MACRO or create_function (connection-scoped, would be lost).
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from proto_pipe.macros.registry import (
    MacroContract,
    MacroRegistry,
    parse_macro_signature,
)

if TYPE_CHECKING:
    import duckdb


def load_sql_macros(
    conn: duckdb.DuckDBPyConnection,
    macros_dir: str,
    macro_registry: MacroRegistry,
) -> None:
    """Load SQL macro files from macros_dir into DuckDB and the registry.

    For each .sql file:
    1. Parse the macro signature → MacroContract(source="sql")
    2. Register in MacroRegistry
    3. Execute the SQL against conn (registers CREATE MACRO in DuckDB)

    Files that fail to parse or execute are skipped with a warning.
    Missing directory warns but doesn't crash.
    """
    p = Path(macros_dir)
    if not p.exists():
        print(f"  [warn] macros_dir '{macros_dir}' not found — skipping macro loading")
        return

    sql_files = sorted(p.glob("*.sql"))
    if not sql_files:
        return

    for sql_file in sql_files:
        try:
            sql = sql_file.read_text().strip()
            if not sql:
                continue

            # Parse signature for registry
            parsed = parse_macro_signature(sql)
            if parsed:
                name, params = parsed
                contract = MacroContract(
                    name=name,
                    params=params,
                    param_types={},  # SQL macros are untyped
                    return_type=None,
                    source="sql",
                    func=None,
                    func_name=name,
                )
                macro_registry.register(contract)

            # Execute in DuckDB regardless of parse success
            conn.execute(sql)
            print(f"  [macro] Loaded '{sql_file.name}'")
        except Exception as e:
            print(f"  [macro-fail] '{sql_file.name}': {e} — skipped")


def register_python_macros(
    conn: duckdb.DuckDBPyConnection,
    macro_registry: MacroRegistry,
) -> None:
    """Register all Python macros from the registry as DuckDB UDFs.

    Iterates MacroRegistry for source='python' contracts and calls
    conn.create_function() for each.  conn.create_function accepts
    Python types directly (float, int, str, bool) — no DUCKDB_TYPE_MAP
    needed.  Unannotated params default to str.
    """
    for name in macro_registry.available():
        contract = macro_registry.get_contract(name)
        if contract is None or contract.source != "python" or contract.func is None:
            continue

        # Map Python type annotations — unannotated defaults to str
        param_duckdb_types = []
        for param_name in contract.params:
            py_type = contract.param_types.get(param_name, str)
            if py_type not in (int, float, str, bool):
                print(
                    f"  [macro-udf-skip] '{name}': param '{param_name}' has "
                    f"unmappable type {py_type.__name__} — skipped"
                )
                break
            param_duckdb_types.append(py_type)
        else:
            # All params mapped successfully
            return_py_type = contract.return_type or str

            if return_py_type not in (int, float, str, bool):
                print(
                    f"  [macro-udf-skip] '{name}': return type "
                    f"{return_py_type.__name__} is unmappable — skipped"
                )
                continue

            try:
                conn.create_function(
                    name,
                    contract.func,
                    param_duckdb_types,
                    return_py_type,
                )
                print(f"  [macro-udf] Registered '{name}' as DuckDB function")
            except Exception as e:
                print(f"  [macro-udf-fail] '{name}': {e}")


def load_all_macros(
    conn: duckdb.DuckDBPyConnection,
    settings: dict,
) -> MacroRegistry:
    """Orchestrate full macro loading on a live connection.

    Called by vp deliver and vp run-all before deliverable execution.
    Returns the populated MacroRegistry (for wizard use or inspection).
    """
    macro_registry = MacroRegistry()

    # Python macros from user module
    module_path = settings.get("custom_macros_module")
    if module_path:
        from proto_pipe.macros.helpers import load_macros_module
        load_macros_module(module_path, macro_registry)

    # SQL macros from files
    macros_dir = settings.get("macros_dir")
    if macros_dir:
        load_sql_macros(conn, macros_dir, macro_registry)

    # Register Python macros as DuckDB UDFs
    register_python_macros(conn, macro_registry)

    return macro_registry


def smoke_test_macros(settings: dict) -> None:
    """Parse-only validation of macro files — no DuckDB registration.

    Called by vp init db to surface config errors early.  Does NOT
    execute CREATE MACRO or create_function (connection-scoped, would
    be lost when init db closes).
    """
    from proto_pipe.macros.helpers import DECORATED_MACROS, load_macros_module
    from proto_pipe.macros.registry import MacroRegistry, validate_macro

    macro_registry = MacroRegistry()

    # Validate Python macros (import + validate_macro, no UDF registration)
    module_path = settings.get("custom_macros_module")
    if module_path:
        load_macros_module(module_path, macro_registry)

    # Validate SQL macros (parse signatures only, no conn.execute)
    macros_dir = settings.get("macros_dir")
    if macros_dir:
        p = Path(macros_dir)
        if not p.exists():
            print(f"  [warn] macros_dir '{macros_dir}' not found")
            return

        for sql_file in sorted(p.glob("*.sql")):
            try:
                sql = sql_file.read_text().strip()
                if not sql:
                    continue
                parsed = parse_macro_signature(sql)
                if parsed:
                    name, params = parsed
                    print(f"  [macro-ok] '{sql_file.name}': {name}({', '.join(params)})")
                else:
                    print(f"  [macro-warn] '{sql_file.name}': could not parse signature")
            except Exception as e:
                print(f"  [macro-fail] '{sql_file.name}': {e}")
