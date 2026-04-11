"""Tests for macro loading — loader.py, helpers.py, registry.py integration.

Tests:
- register_python_macros makes a @macro-decorated function callable in DuckDB
- load_sql_macros + register_python_macros together in same connection
- Missing custom_macros_module path → no crash, no macros loaded
- Python macro with unmappable type → skipped with warning
"""
from pathlib import Path

import duckdb
import pytest

from proto_pipe.macros.registry import (
    MacroContract,
    MacroRegistry,
    validate_macro,
    parse_macro_signature,
)
from proto_pipe.macros.loader import (
    load_sql_macros,
    register_python_macros,
    load_all_macros,
    smoke_test_macros,
)
from proto_pipe.macros.helpers import macro, DECORATED_MACROS, load_macros_module


@pytest.fixture(autouse=True)
def _clear_decorated_macros():
    """Reset the global staging dict between tests."""
    DECORATED_MACROS.clear()
    yield
    DECORATED_MACROS.clear()


# ---------------------------------------------------------------------------
# Test: Python macro callable in DuckDB query
# ---------------------------------------------------------------------------
class TestRegisterPythonMacros:
    def test_python_macro_callable_in_duckdb(self):
        """A @macro-decorated function is callable via SELECT after registration."""

        def apply_quota_share(premium: float, share: float) -> float:
            return premium * share

        contract = validate_macro(apply_quota_share)
        contract = MacroContract(
            name="apply_quota_share",
            params=contract.params,
            param_types=contract.param_types,
            return_type=contract.return_type,
            source="python",
            func=apply_quota_share,
            func_name=contract.func_name,
        )

        registry = MacroRegistry()
        registry.register(contract)

        conn = duckdb.connect(":memory:")
        register_python_macros(conn, registry)

        result = conn.execute("SELECT apply_quota_share(100.0, 0.25)").fetchone()[0]
        assert result == pytest.approx(25.0)
        conn.close()

    def test_unannotated_params_default_to_str(self):
        """Unannotated params default to str and work in DuckDB."""

        def upper_name(val) -> str:
            return val.upper()

        contract = validate_macro(upper_name)
        contract = MacroContract(
            name="upper_name",
            params=contract.params,
            param_types=contract.param_types,
            return_type=contract.return_type,
            source="python",
            func=upper_name,
            func_name=contract.func_name,
        )

        registry = MacroRegistry()
        registry.register(contract)

        conn = duckdb.connect(":memory:")
        register_python_macros(conn, registry)

        result = conn.execute("SELECT upper_name('hello')").fetchone()[0]
        assert result == "HELLO"
        conn.close()


# ---------------------------------------------------------------------------
# Test: SQL + Python macros together
# ---------------------------------------------------------------------------
class TestSqlAndPythonMacrosTogether:
    def test_both_macro_types_available_in_same_connection(self, tmp_path):
        """SQL and Python macros coexist on the same connection."""
        # SQL macro
        macros_dir = tmp_path / "macros"
        macros_dir.mkdir()
        (macros_dir / "double_it.sql").write_text(
            "CREATE OR REPLACE MACRO double_it(x) AS x * 2;"
        )

        # Python macro
        def triple_it(x: float) -> float:
            return x * 3.0

        py_contract = validate_macro(triple_it)
        py_contract = MacroContract(
            name="triple_it",
            params=py_contract.params,
            param_types=py_contract.param_types,
            return_type=py_contract.return_type,
            source="python",
            func=triple_it,
            func_name=py_contract.func_name,
        )

        registry = MacroRegistry()
        registry.register(py_contract)

        conn = duckdb.connect(":memory:")
        load_sql_macros(conn, str(macros_dir), registry)
        register_python_macros(conn, registry)

        # Both should work
        sql_result = conn.execute("SELECT double_it(5)").fetchone()[0]
        py_result = conn.execute("SELECT triple_it(5.0)").fetchone()[0]
        assert sql_result == 10
        assert py_result == pytest.approx(15.0)

        # Registry knows about both
        assert "double_it" in registry.available()
        assert "triple_it" in registry.available()
        conn.close()


# ---------------------------------------------------------------------------
# Test: Missing custom_macros_module → no crash
# ---------------------------------------------------------------------------
class TestMissingMacrosModule:
    def test_missing_module_path_no_crash(self):
        """load_all_macros with no custom_macros_module and no macros_dir
        returns an empty registry without errors."""
        conn = duckdb.connect(":memory:")
        settings = {}  # no custom_macros_module, no macros_dir

        registry = load_all_macros(conn, settings)

        assert registry.available() == []
        conn.close()

    def test_missing_macros_dir_no_crash(self, capsys):
        """Nonexistent macros_dir warns but doesn't crash."""
        conn = duckdb.connect(":memory:")
        settings = {"macros_dir": "/nonexistent/macros"}

        registry = load_all_macros(conn, settings)

        assert registry.available() == []
        captured = capsys.readouterr()
        assert "not found" in captured.out
        conn.close()


# ---------------------------------------------------------------------------
# Test: Unmappable type → skipped with warning
# ---------------------------------------------------------------------------
class TestUnmappableType:
    def test_unmappable_param_type_skipped_with_warning(self, capsys):
        """A macro with a non-scalar param type is skipped, not registered."""
        import datetime

        def bad_macro(val: datetime.datetime) -> str:
            return str(val)

        contract = validate_macro(bad_macro)
        contract = MacroContract(
            name="bad_macro",
            params=contract.params,
            param_types=contract.param_types,
            return_type=contract.return_type,
            source="python",
            func=bad_macro,
            func_name=contract.func_name,
        )

        registry = MacroRegistry()
        registry.register(contract)

        conn = duckdb.connect(":memory:")
        register_python_macros(conn, registry)

        captured = capsys.readouterr()
        assert "unmappable" in captured.out
        assert "bad_macro" in captured.out

        # Should NOT be callable in DuckDB
        with pytest.raises(Exception):
            conn.execute("SELECT bad_macro('test')")
        conn.close()

    def test_unmappable_return_type_skipped(self, capsys):
        """A macro with unmappable return type is skipped."""
        import datetime

        def bad_return(val: str) -> datetime.date:
            return datetime.date.today()

        contract = validate_macro(bad_return)
        contract = MacroContract(
            name="bad_return",
            params=contract.params,
            param_types=contract.param_types,
            return_type=contract.return_type,
            source="python",
            func=bad_return,
            func_name=contract.func_name,
        )

        registry = MacroRegistry()
        registry.register(contract)

        conn = duckdb.connect(":memory:")
        register_python_macros(conn, registry)

        captured = capsys.readouterr()
        assert "unmappable" in captured.out
        conn.close()


# ---------------------------------------------------------------------------
# Test: smoke_test_macros (parse-only, no connection)
# ---------------------------------------------------------------------------
class TestSmokeTestMacros:
    def test_smoke_test_validates_sql_files(self, tmp_path, capsys):
        """smoke_test_macros parses SQL files without executing."""
        macros_dir = tmp_path / "macros"
        macros_dir.mkdir()
        (macros_dir / "my_macro.sql").write_text(
            "CREATE OR REPLACE MACRO my_macro(a, b) AS a + b;"
        )

        smoke_test_macros({"macros_dir": str(macros_dir)})

        captured = capsys.readouterr()
        assert "my_macro" in captured.out
        assert "macro-ok" in captured.out

    def test_smoke_test_warns_on_unparseable(self, tmp_path, capsys):
        """smoke_test_macros warns on files it can't parse."""
        macros_dir = tmp_path / "macros"
        macros_dir.mkdir()
        (macros_dir / "bad.sql").write_text("SELECT 1;")

        smoke_test_macros({"macros_dir": str(macros_dir)})

        captured = capsys.readouterr()
        assert "macro-warn" in captured.out


# ---------------------------------------------------------------------------
# Test: load_all_macros orchestrator
# ---------------------------------------------------------------------------
class TestLoadAllMacros:
    def test_sql_macros_loaded_via_orchestrator(self, tmp_path):
        """load_all_macros loads SQL macros and makes them queryable."""
        macros_dir = tmp_path / "macros"
        macros_dir.mkdir()
        (macros_dir / "add_ten.sql").write_text(
            "CREATE OR REPLACE MACRO add_ten(x) AS x + 10;"
        )

        conn = duckdb.connect(":memory:")
        registry = load_all_macros(conn, {"macros_dir": str(macros_dir)})

        result = conn.execute("SELECT add_ten(5)").fetchone()[0]
        assert result == 15
        assert "add_ten" in registry.available()
        conn.close()
