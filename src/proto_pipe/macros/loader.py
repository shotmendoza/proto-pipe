"""Macro loading — SQL file loading and Python UDF registration.

load_sql_macros: reads .sql files, parses signatures, registers in
    MacroRegistry, executes CREATE MACRO in DuckDB.
register_python_macros: iterates MacroRegistry for source='python'
    contracts, calls conn.create_function for each.
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from proto_pipe.constants import DUCKDB_TYPE_MAP
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
    conn.create_function() for each, using DUCKDB_TYPE_MAP for type mapping.
    """
    for name in macro_registry.available():
        contract = macro_registry.get_contract(name)
        if contract is None or contract.source != "python" or contract.func is None:
            continue

        # Map Python type annotations to DuckDB types
        param_duckdb_types = []
        for param_name in contract.params:
            py_type = contract.param_types.get(param_name, str)
            param_duckdb_types.append(py_type)

        return_py_type = contract.return_type or str

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
