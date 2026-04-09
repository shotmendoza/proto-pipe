"""
Tests for:
- alias_map expansion (test_alias_map_*)
- reset_report (test_reset_*)
- load_macros / vp new-macro (test_macro_*)
"""

import duckdb
import pandas as pd
import pytest

from proto_pipe.checks.built_in import BUILT_IN_CHECKS
from proto_pipe.checks.helpers import DECORATED_CHECKS, custom_check, register_custom_check
from proto_pipe.io.db import init_ingest_state, init_all_pipeline_tables, table_exists
from proto_pipe.io.ingest import (
    init_db,
    ingest_directory,
    load_macros,
    reset_report,
)
from proto_pipe.io.registry import (
    _build_alias_param_map,
    _expand_check_with_alias_map,
    register_from_config,
)
from proto_pipe.checks.registry import CheckRegistry, ReportRegistry, CheckContract, validate_check


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture(autouse=True)
def restore_built_in_checks():
    """Restore BUILT_IN_CHECKS to its original state after each test.

    Tests that call register_custom_check mutate the global BUILT_IN_CHECKS.
    Without this fixture, a check registered in one test (e.g. an unavailable
    lambda) would be picked up by _fresh_registry() in later tests and cause
    spurious failures.
    """
    original = dict(BUILT_IN_CHECKS)
    yield
    BUILT_IN_CHECKS.clear()
    BUILT_IN_CHECKS.update(original)


def _fresh_registry() -> CheckRegistry:
    reg = CheckRegistry()
    for name, func in BUILT_IN_CHECKS.items():
        reg.register(name, func)
    return reg


def _setup_table(db_path: str, table_name: str, df: pd.DataFrame) -> None:
    conn = duckdb.connect(db_path)
    conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM df')
    conn.close()


def _read_table(db_path: str, table_name: str) -> pd.DataFrame:
    conn = duckdb.connect(db_path)
    df = conn.execute(f'SELECT * FROM "{table_name}"').df()
    conn.close()
    return df


def _setup_db_with_table(db_path: str, table_name: str, df: pd.DataFrame) -> None:
    init_db(db_path)
    conn = duckdb.connect(db_path)
    init_ingest_state(conn)
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" AS SELECT * FROM df')
    conn.execute("""
        INSERT INTO ingest_state (id, filename, table_name, status, rows, ingested_at)
        VALUES (gen_random_uuid()::VARCHAR, 'sales_2026.csv', ?, 'ok', 3, NOW())
    """, [table_name])
    conn.close()


# ===========================================================================
# RegistryEntry dataclass
# ===========================================================================

def test_registry_entry_defaults():
    def dummy(ctx): pass
    entry = CheckContract(func=dummy)
    assert entry.kind == "check"
    assert entry.func is dummy


def test_registry_entry_transform_kind():
    def dummy(ctx): pass
    entry = CheckContract(func=dummy, kind="transform")
    assert entry.kind == "transform"


# ===========================================================================
# CheckRegistry with kind support
# ===========================================================================

def test_registry_stores_registry_entry():
    reg = CheckRegistry()

    def my_check(ctx: dict) -> pd.Series:
        return pd.Series([True])

    reg.register("my_check", my_check)
    assert isinstance(reg._checks["my_check"], CheckContract)


def test_registry_get_returns_callable():
    reg = CheckRegistry()

    def my_check(ctx: dict) -> pd.Series:
        return pd.Series([True])

    reg.register("my_check", my_check)
    func = reg.get("my_check")
    assert callable(func)


def test_registry_get_missing_returns_none():
    reg = CheckRegistry()
    assert reg.get("nonexistent") is None


def test_registry_get_kind_check():
    reg = CheckRegistry()

    def my_check(ctx: dict) -> pd.Series:
        return pd.Series([True])

    reg.register("my_check", my_check, kind="check")
    assert reg.get_kind("my_check") == "check"


def test_registry_get_kind_transform():
    reg = CheckRegistry()

    def my_transform(ctx: dict) -> pd.Series:
        return ctx["df"]["val"]

    reg.register("my_transform", my_transform, kind="transform")
    assert reg.get_kind("my_transform") == "transform"


def test_registry_get_kind_missing_raises():
    reg = CheckRegistry()
    with pytest.raises(ValueError, match="No check registered"):
        reg.get_kind("nonexistent")


def test_registry_checks_only():
    reg = CheckRegistry()

    def c(ctx: dict) -> pd.Series: return pd.Series([True])
    def t(ctx): return ctx["df"]

    reg.register("check_a", c, kind="check")
    reg.register("transform_a", t, kind="transform")
    reg.register("check_b", c, kind="check")

    assert set(reg.checks_only()) == {"check_a", "check_b"}
    assert reg.transforms_only() == ["transform_a"]


def test_registry_run_unpacks_entry():
    reg = CheckRegistry()

    def my_check(ctx: dict) -> pd.Series:
        return pd.Series([True, False])

    reg.register("my_check", my_check)
    # run should return a CheckResult (wrapped), not raise
    result = reg.run("my_check", {"df": pd.DataFrame({"a": [1, 2]})})
    assert result is not None


def test_invalid_kind_skips_registration(capsys):
    """Invalid kind should warn and skip registration, not raise."""

    def my_check(ctx: dict) -> pd.Series:
        return pd.Series([True])

    audit = validate_check("my_check", my_check, kind="invalid")
    assert audit.contract is None
    assert "warn" in capsys.readouterr().out


# ===========================================================================
# @custom_check decorator stores (func, kind) tuple
# ===========================================================================

def test_custom_check_decorator_stores_tuple():
    @custom_check("_test_check_tuple")
    def my_check(ctx: dict) -> pd.Series:
        return pd.Series([True])

    assert "_test_check_tuple" in DECORATED_CHECKS
    func, kind = DECORATED_CHECKS["_test_check_tuple"]
    assert callable(func)
    assert kind == "check"


def test_custom_check_decorator_transform_kind():
    @custom_check("_test_transform_tuple", kind="transform")
    def my_transform(ctx: dict) -> pd.Series:
        return ctx["df"]["val"]

    func, kind = DECORATED_CHECKS["_test_transform_tuple"]
    assert kind == "transform"


def test_custom_check_invalid_kind_raises():
    with pytest.raises(ValueError, match="kind must be"):
        @custom_check("_bad_kind", kind="invalid")
        def noop(ctx): pass


# ===========================================================================
# register_custom_check validates return type
# ===========================================================================

def test_register_custom_check_bad_annotation_skips_registration(capsys):
    """kind='check' without pd.Series return annotation should warn and skip registration."""
    reg = CheckRegistry()

    def bad_check(ctx: dict):  # no return annotation
        return ctx["df"]

    register_custom_check("_bad_check", bad_check, reg, kind="check")
    captured = capsys.readouterr()
    assert "warn" in captured.out
    assert "_bad_check" not in reg.available()


def test_register_custom_check_transform_bool_annotation_warns(capsys):
    """kind='transform' with pd.Series[bool] return annotation should warn but still register."""
    reg = _fresh_registry()

    def looks_like_check(ctx: dict) -> pd.Series:
        return pd.Series([True])

    register_custom_check("_looks_like_check", looks_like_check, reg, kind="transform")
    captured = capsys.readouterr()
    assert "warn" in captured.out
    assert "_looks_like_check" in reg.available()


# ===========================================================================
# Alias map — _build_alias_param_map
# ===========================================================================

def test_build_alias_param_map_single_entry():
    result = _build_alias_param_map([{"param": "col", "column": "Coverage A"}])
    assert result == {"col": ["Coverage A"]}


def test_build_alias_param_map_multi_entry_same_param():
    alias_map = [
        {"param": "col", "column": "Coverage A"},
        {"param": "col", "column": "Coverage B"},
    ]
    result = _build_alias_param_map(alias_map)
    assert result == {"col": ["Coverage A", "Coverage B"]}


def test_build_alias_param_map_multiple_params():
    alias_map = [
        {"param": "col", "column": "Coverage A"},
        {"param": "bound_premium", "column": "Foo Bound Premium"},
    ]
    result = _build_alias_param_map(alias_map)
    assert result["col"] == ["Coverage A"]
    assert result["bound_premium"] == ["Foo Bound Premium"]


def test_build_alias_param_map_empty():
    assert _build_alias_param_map([]) == {}


# ===========================================================================
# Alias map — _expand_check_with_alias_map
# ===========================================================================

def test_expand_no_alias_map_registers_single():
    reg = _fresh_registry()
    names = _expand_check_with_alias_map(
        func_name="range_check",
        params={"col": "price", "min_val": 0, "max_val": 500},
        alias_param_map={},
        check_registry=reg,
    )
    assert len(names) == 1


def test_expand_single_column_alias_registers_one():
    reg = _fresh_registry()
    names = _expand_check_with_alias_map(
        func_name="range_check",
        params={"min_val": 0, "max_val": 500},
        alias_param_map={"col": ["Coverage A"]},
        check_registry=reg,
    )
    assert len(names) == 1
    df = pd.DataFrame({"Coverage A": [100.0, 200.0]})
    result = reg.run(names[0], {"df": df})
    assert result is not None


def test_expand_multi_column_alias_registers_n():
    reg = _fresh_registry()
    names = _expand_check_with_alias_map(
        func_name="range_check",
        params={"min_val": 0, "max_val": 500},
        alias_param_map={"col": ["Coverage A", "Coverage B", "Coverage C"]},
        check_registry=reg,
    )
    assert len(names) == 3
    assert len(set(names)) == 3  # all distinct


def test_expand_scalar_params_broadcast():
    """min_val and max_val should be baked into every expanded check."""
    reg = _fresh_registry()
    names = _expand_check_with_alias_map(
        func_name="range_check",
        params={"min_val": 10, "max_val": 100},
        alias_param_map={"col": ["Col A", "Col B"]},
        check_registry=reg,
    )
    assert len(names) == 2
    df_pass = pd.DataFrame({"Col A": [50.0]})
    df_fail = pd.DataFrame({"Col A": [5.0]})
    result_pass = reg.run(names[0], {"df": df_pass})
    result_fail = reg.run(names[0], {"df": df_fail})
    assert result_pass.passed is True
    assert result_fail.passed is False


def test_expand_idempotent():
    reg = _fresh_registry()
    names1 = _expand_check_with_alias_map(
        func_name="range_check",
        params={"min_val": 0, "max_val": 500},
        alias_param_map={"col": ["Coverage A"]},
        check_registry=reg,
    )
    names2 = _expand_check_with_alias_map(
        func_name="range_check",
        params={"min_val": 0, "max_val": 500},
        alias_param_map={"col": ["Coverage A"]},
        check_registry=reg,
    )
    assert names1 == names2


def test_expand_unknown_param_in_alias_map_ignored():
    reg = _fresh_registry()
    names = _expand_check_with_alias_map(
        func_name="range_check",
        params={"col": "price", "min_val": 0, "max_val": 500},
        alias_param_map={"unknown_param": ["X", "Y"]},
        check_registry=reg,
    )
    assert len(names) == 1


def test_expand_unequal_multi_column_lengths_raises():
    reg = _fresh_registry()

    def two_col_check(context: dict, col_a: str, col_b: str) -> pd.Series:
        df = context["df"]
        return df[col_a] == df[col_b]

    BUILT_IN_CHECKS["_two_col_check"] = two_col_check
    reg.register("_two_col_check", two_col_check)

    with pytest.raises(ValueError, match="unequal lengths"):
        _expand_check_with_alias_map(
            func_name="_two_col_check",
            params={},
            alias_param_map={
                "col_a": ["A1", "A2", "A3"],
                "col_b": ["B1", "B2"],
            },
            check_registry=reg,
        )


def test_expand_single_entry_broadcasts_silently():
    reg = _fresh_registry()

    def two_col_check(context: dict, col_a: str, col_b: str) -> pd.Series:
        df = context["df"]
        return df[col_a] == df[col_b]

    BUILT_IN_CHECKS["_two_col_broadcast"] = two_col_check
    reg.register("_two_col_broadcast", two_col_check)

    names = _expand_check_with_alias_map(
        func_name="_two_col_broadcast",
        params={},
        alias_param_map={
            "col_a": ["A1", "A2", "A3"],
            "col_b": ["B1"],            # single entry broadcasts
        },
        check_registry=reg,
    )
    assert len(names) == 3


def test_register_from_config_alias_map_expands_resolved_checks():
    config = {
        "templates": {},
        "reports": [
            {
                "name": "coverage_report",
                "source": {"type": "duckdb", "path": "", "table": "sales"},
                "alias_map": [
                    {"param": "col", "column": "Coverage A"},
                    {"param": "col", "column": "Coverage B"},
                ],
                "options": {"parallel": False},
                "checks": [
                    {"name": "range_check", "params": {"min_val": 0, "max_val": 1000}},
                ],
            }
        ],
    }
    reg = _fresh_registry()
    rep_reg = ReportRegistry()
    register_from_config(config, reg, rep_reg)

    report = rep_reg.get("coverage_report")
    assert len(report["resolved_checks"]) == 2
    assert len(set(report["resolved_checks"])) == 2


def test_register_from_config_no_alias_map_single_check():
    config = {
        "templates": {},
        "reports": [
            {
                "name": "sales_report",
                "source": {"type": "duckdb", "path": "", "table": "sales"},
                "options": {"parallel": False},
                "checks": [
                    {"name": "range_check", "params": {"col": "price", "min_val": 0, "max_val": 500}},
                ],
            }
        ],
    }
    reg = _fresh_registry()
    rep_reg = ReportRegistry()
    register_from_config(config, reg, rep_reg)

    report = rep_reg.get("sales_report")
    assert len(report["resolved_checks"]) == 1


def test_register_from_config_alias_map_stored_in_report():
    alias_map = [
        {"param": "col", "column": "Coverage A"},
        {"param": "col", "column": "Coverage B"},
    ]
    config = {
        "templates": {},
        "reports": [
            {
                "name": "test_report",
                "source": {"type": "duckdb", "path": "", "table": "t"},
                "alias_map": alias_map,
                "options": {"parallel": False},
                "checks": [
                    {"name": "range_check", "params": {"min_val": 0, "max_val": 100}},
                ],
            }
        ],
    }
    reg = _fresh_registry()
    rep_reg = ReportRegistry()
    register_from_config(config, reg, rep_reg)

    report = rep_reg.get("test_report")
    assert report["alias_map"] == alias_map


def test_run_report_transforms_run_after_checks(tmp_path):
    """Checks must complete before transforms are applied."""
    from proto_pipe.reports.runner import run_report
    from proto_pipe.pipelines.watermark import WatermarkStore

    db_path = str(tmp_path / "pipeline.db")
    wm_path = str(tmp_path / "watermarks.db")

    init_db(db_path)
    conn = duckdb.connect(db_path)
    from proto_pipe.io.db import init_all_pipeline_tables
    conn_all = duckdb.connect(db_path)
    init_all_pipeline_tables(conn_all)
    conn_all.close()
    conn = duckdb.connect(db_path)
    df = pd.DataFrame({
        "id": ["R1", "R2"],
        "status": ["Issuance", "Renewal"],
        "_ingested_at": pd.Timestamp("2026-01-01", tz="UTC"),
    })
    conn.execute("CREATE TABLE sales AS SELECT * FROM df")
    conn.close()

    reg = CheckRegistry()
    call_order = []

    def my_check(context: dict) -> pd.Series:
        call_order.append("check")
        return pd.Series([True, True])

    def my_transform(context: dict) -> pd.Series:
        call_order.append("transform")
        s = context["df"]["status"].replace({"Issuance": "Reinstatement"})
        s.name = "status"
        return s

    reg.register("my_check", my_check, kind="check")
    reg.register("my_transform", my_transform, kind="transform")

    wm = WatermarkStore(wm_path)
    report_config = {
        "name": "test_report",
        "source": {
            "type": "duckdb", "path": db_path,
            "table": "sales", "timestamp_col": "_ingested_at",
        },
        "options": {"parallel": False},
        "resolved_checks": ["my_check", "my_transform"],
    }

    run_report(report_config, reg, wm, pipeline_db=db_path)
    assert call_order == ["check", "transform"]


# ===========================================================================
# reset_report
# ===========================================================================

def test_reset_report_drops_table(tmp_path):
    db_path = str(tmp_path / "pipeline.db")
    df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    _setup_db_with_table(db_path, "sales", df)

    reset_report("sales", db_path)

    conn = duckdb.connect(db_path)
    assert not table_exists(conn, "sales")
    conn.close()


def test_reset_report_clears_ingest_state(tmp_path):
    db_path = str(tmp_path / "pipeline.db")
    df = pd.DataFrame({"id": [1], "val": ["x"]})
    _setup_db_with_table(db_path, "sales", df)

    reset_report("sales", db_path)

    conn = duckdb.connect(db_path)
    count = conn.execute(
        "SELECT count(*) FROM ingest_state WHERE table_name = 'sales'"
    ).fetchone()[0]
    conn.close()
    assert count == 0


def test_reset_report_nonexistent_table_is_graceful(tmp_path):
    db_path = str(tmp_path / "pipeline.db")
    init_db(db_path)
    reset_report("nonexistent_table", db_path)  # should not raise


def test_reset_report_does_not_affect_other_tables(tmp_path):
    db_path = str(tmp_path / "pipeline.db")
    init_db(db_path)
    conn = duckdb.connect(db_path)
    init_ingest_state(conn)
    conn.execute("""
        INSERT INTO ingest_state (id, filename, table_name, status, rows, ingested_at)
        VALUES
            (gen_random_uuid()::VARCHAR, 'sales.csv', 'sales', 'ok', 3, NOW()),
            (gen_random_uuid()::VARCHAR, 'inventory.csv', 'inventory', 'ok', 5, NOW())
    """)
    conn.close()

    reset_report("sales", db_path)

    conn = duckdb.connect(db_path)
    inventory_count = conn.execute(
        "SELECT count(*) FROM ingest_state WHERE table_name = 'inventory'"
    ).fetchone()[0]
    conn.close()
    assert inventory_count == 1


def test_reset_report_allows_reingest(tmp_path):
    db_path = str(tmp_path / "pipeline.db")
    incoming = tmp_path / "incoming"
    incoming.mkdir()

    df = pd.DataFrame({"id": ["A", "B"], "val": [1, 2]})
    (incoming / "sales_2026.csv").write_text(df.to_csv(index=False))

    sources = [{"name": "sales", "patterns": ["sales_*.csv"], "target_table": "sales"}]

    init_db(db_path)
    ingest_directory(str(incoming), sources, db_path)

    reset_report("sales", db_path)

    conn = duckdb.connect(db_path)
    assert not table_exists(conn, "sales")
    conn.close()

    ingest_directory(str(incoming), sources, db_path)

    conn = duckdb.connect(db_path)
    assert table_exists(conn, "sales")
    rows = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
    conn.close()
    assert rows == 2


# ===========================================================================
# load_macros
# ===========================================================================

def test_load_macros_registers_macro(tmp_path):
    macros_dir = tmp_path / "macros"
    macros_dir.mkdir()
    (macros_dir / "normalize.sql").write_text("""
        CREATE OR REPLACE MACRO normalize_status(val) AS
            CASE WHEN val = 'Issuance' THEN 'Reinstatement' ELSE val END;
    """)
    conn = duckdb.connect()
    load_macros(conn, str(macros_dir))

    assert conn.execute("SELECT normalize_status('Issuance')").fetchone()[0] == "Reinstatement"
    assert conn.execute("SELECT normalize_status('Renewal')").fetchone()[0] == "Renewal"
    conn.close()


def test_load_macros_missing_dir_warns_not_crashes(tmp_path, capsys):
    conn = duckdb.connect()
    load_macros(conn, str(tmp_path / "nonexistent"))
    assert "warn" in capsys.readouterr().out
    conn.close()


def test_load_macros_empty_dir_is_noop(tmp_path):
    macros_dir = tmp_path / "macros"
    macros_dir.mkdir()
    conn = duckdb.connect()
    load_macros(conn, str(macros_dir))  # should not raise
    conn.close()


def test_load_macros_broken_file_skipped_others_load(tmp_path, capsys):
    macros_dir = tmp_path / "macros"
    macros_dir.mkdir()
    (macros_dir / "bad.sql").write_text("THIS IS NOT VALID SQL ;;;")
    (macros_dir / "good.sql").write_text("CREATE OR REPLACE MACRO double_val(x) AS x * 2;")

    conn = duckdb.connect()
    load_macros(conn, str(macros_dir))

    assert conn.execute("SELECT double_val(5)").fetchone()[0] == 10
    assert "macro-fail" in capsys.readouterr().out
    conn.close()


def test_load_macros_idempotent(tmp_path):
    macros_dir = tmp_path / "macros"
    macros_dir.mkdir()
    (macros_dir / "add_one.sql").write_text("CREATE OR REPLACE MACRO add_one(x) AS x + 1;")

    conn = duckdb.connect()
    load_macros(conn, str(macros_dir))
    load_macros(conn, str(macros_dir))

    assert conn.execute("SELECT add_one(10)").fetchone()[0] == 11
    conn.close()


# ===========================================================================
# vp new-macro CLI
# ===========================================================================

def test_new_macro_creates_sql_file(tmp_path):
    from click.testing import CliRunner
    from proto_pipe.cli.commands.new import new_macro

    macros_dir = tmp_path / "macros"
    runner = CliRunner()
    result = runner.invoke(new_macro, ["normalize_tx", "--macros-dir", str(macros_dir)])

    assert result.exit_code == 0
    macro_file = macros_dir / "normalize_tx.sql"
    assert macro_file.exists()
    content = macro_file.read_text()
    assert "CREATE OR REPLACE MACRO" in content
    assert "normalize_tx" in content


def test_new_macro_skips_existing_file(tmp_path):
    from click.testing import CliRunner
    from proto_pipe.cli.commands.new import new_macro

    macros_dir = tmp_path / "macros"
    macros_dir.mkdir()
    existing = macros_dir / "my_macro.sql"
    existing.write_text("-- existing content")

    runner = CliRunner()
    result = runner.invoke(new_macro, ["my_macro", "--macros-dir", str(macros_dir)])

    assert result.exit_code == 0
    assert "skip" in result.output
    assert existing.read_text() == "-- existing content"


# ===========================================================================
# Spec behavioral guarantee tests
# ===========================================================================

def test_bad_checks_populated_on_failed_registration(capsys):
    """Failed registrations go to _bad_checks, not _checks.

    Spec guarantee:
      'CheckRegistry._checks holds only vetted contracts.
       CheckRegistry._bad_checks holds {name: reason} for failures.'
    """
    reg = CheckRegistry()

    def bad_check(ctx):  # no return annotation — fails validation
        return ctx["df"]

    register_custom_check("_bad_check_test", bad_check, reg, kind="check")

    assert "_bad_check_test" not in reg._checks, (
        "Failed check must not be in _checks"
    )
    assert "_bad_check_test" in reg._bad_checks, (
        "Failed check must be recorded in _bad_checks"
    )
    assert reg._bad_checks["_bad_check_test"], (
        "_bad_checks must record a reason string"
    )


def test_cross_source_aliasing_same_check_different_columns():
    """Same check function runs against different column names in separate reports.

    Spec guarantee (alias_map purpose 1):
      'Cross-source aliasing — same check reused across sources that name
       the same concept differently. col → endt in van, col → expiry_date in foo.'
    """
    config = {
        "templates": {},
        "reports": [
            {
                "name": "van_report",
                "source": {"type": "duckdb", "path": "", "table": "van"},
                "alias_map": [{"param": "col", "column": "endt"}],
                "options": {"parallel": False},
                "checks": [{"name": "range_check", "params": {"min_val": 0, "max_val": 1000}}],
            },
            {
                "name": "foo_report",
                "source": {"type": "duckdb", "path": "", "table": "foo"},
                "alias_map": [{"param": "col", "column": "expiry_date_val"}],
                "options": {"parallel": False},
                "checks": [{"name": "range_check", "params": {"min_val": 0, "max_val": 1000}}],
            },
        ],
    }
    reg = _fresh_registry()
    rep_reg = ReportRegistry()
    register_from_config(config, reg, rep_reg)

    van = rep_reg.get("van_report")
    foo = rep_reg.get("foo_report")

    assert len(van["resolved_checks"]) == 1
    assert len(foo["resolved_checks"]) == 1

    # The two reports must produce different registered check names
    assert van["resolved_checks"][0] != foo["resolved_checks"][0], (
        "Cross-source aliasing must produce distinct registered check instances"
    )

    # Each check must actually target the right column
    df_van = pd.DataFrame({"endt": [500.0]})
    df_foo = pd.DataFrame({"expiry_date_val": [500.0]})

    result_van = reg.run(van["resolved_checks"][0], {"df": df_van})
    result_foo = reg.run(foo["resolved_checks"][0], {"df": df_foo})

    assert result_van.passed is True, "van check must pass against 'endt' column"
    assert result_foo.passed is True, "foo check must pass against 'expiry_date_val' column"


def test_checkparam_inspector_column_params():
    """CheckParamInspector.column_params() identifies str-annotated params.

    Spec guarantee (CheckParamInspector canonical pattern):
      'column_params() — column selectors: str, "str", pd.Series, "pd.Series",
       or unannotated. Maps to alias_map entries.'
    """
    from proto_pipe.checks.registry import CheckParamInspector

    def check_with_col(col: str, threshold: float) -> pd.Series:
        pass

    inspector = CheckParamInspector(check_with_col)
    col_params = inspector.column_params()
    scalar_params = inspector.scalar_params()

    assert "col" in col_params, "str-annotated param must be a column_param"
    assert "threshold" in scalar_params, "float-annotated param must be a scalar_param"
    assert "threshold" not in col_params, "scalar param must not appear in column_params"


def test_checkparam_inspector_dataframe_params():
    """CheckParamInspector.dataframe_params() identifies pd.DataFrame params.

    Spec guarantee:
      'dataframe_params() — pd.DataFrame annotated params. Auto-filled with
       the full table df at runtime.'
    """
    from proto_pipe.checks.registry import CheckParamInspector

    def check_with_df(col: str, df: pd.DataFrame) -> pd.Series:
        pass

    inspector = CheckParamInspector(check_with_df)
    assert "df" in inspector.dataframe_params(), (
        "pd.DataFrame-annotated param must be identified as a dataframe_param"
    )
    assert "df" not in inspector.column_params(), (
        "DataFrame param must not appear in column_params"
    )


def test_load_custom_checks_module_registers_decorated_checks(tmp_path):
    """load_custom_checks_module loads a file and registers @custom_check functions.

    Spec guarantee:
      '@custom_check(name, kind) decorator stages functions in _DECORATED_CHECKS.
       load_custom_checks_module imports the user module, iterates _DECORATED_CHECKS,
       calls register_custom_check.'
    """
    from proto_pipe.checks.helpers import load_custom_checks_module

    module_path = tmp_path / "my_checks.py"
    module_path.write_text("""
import pandas as pd
from proto_pipe.checks.helpers import custom_check

@custom_check("my_custom_null_check", kind="check")
def my_custom_null_check(col: str) -> pd.Series:
    def _run(ctx):
        return ctx["df"][col].notna()
    return _run
""")

    reg = CheckRegistry()
    load_custom_checks_module(str(module_path), reg)

    assert "my_custom_null_check" in reg.available(), (
        "load_custom_checks_module must register @custom_check decorated functions"
    )


def test_multi_column_expansion_produces_n_results_in_validation_block(tmp_path):
    """Multi-column alias_map expansion produces N result sets in validation_block.

    Spec guarantee (alias_map purpose 2):
      'Multi-column expansion — same check runs once per alias_map entry.
       If col maps to 10 columns, the check runs 10 times, producing 10
       result sets in validation_block.'
    """
    from proto_pipe.io.db import init_all_pipeline_tables
    from proto_pipe.checks.runner import run_checks_and_flag
    import functools

    db_path = str(tmp_path / "pipeline.db")
    conn = duckdb.connect(db_path)
    init_all_pipeline_tables(conn)
    conn.close()

    reg = _fresh_registry()

    # Register two expanded checks (one per column) with failing data
    df = pd.DataFrame({
        "col_a": [-1.0],   # fails range check
        "col_b": [-2.0],   # fails range check
        "order_id": ["ORD-001"],
    })

    from proto_pipe.checks.built_in import check_range
    reg.register(
        "range_col_a",
        functools.partial(check_range, col="col_a", min_val=0, max_val=100),
    )
    reg.register(
        "range_col_b",
        functools.partial(check_range, col="col_b", min_val=0, max_val=100),
    )

    run_checks_and_flag(
        check_names=["range_col_a", "range_col_b"],
        registry=reg,
        context={"df": df},
        pipeline_db=db_path,
        report_name="test_report",
        table_name="sales",
        pk_col="order_id",
    )

    conn = duckdb.connect(db_path)
    count = conn.execute(
        "SELECT count(*) FROM validation_block WHERE report_name = 'test_report'"
    ).fetchone()[0]
    conn.close()

    assert count == 2, (
        f"Two expanded checks must each produce one result in validation_block, got {count}"
    )
