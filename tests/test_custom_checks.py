"""Tests for the custom checks module loading system.

Covers:
- @custom_check decorator registers into _DECORATED_CHECKS
- load_custom_checks_module registers decorated functions into registry + BUILT_IN_CHECKS
- Decorated functions are callable via check_registry.run()
- Missing module path exits cleanly with a message
- Module with no decorated functions emits a warning but doesn't crash
- Custom checks work end-to-end via register_from_config
"""

from pathlib import Path

import pandas as pd
import pytest

from proto_pipe.checks.helpers import custom_check, DECORATED_CHECKS, load_custom_checks_module


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_module(path: Path, content: str) -> None:
    path.write_text(content)


# ---------------------------------------------------------------------------
# @custom_check decorator
# ---------------------------------------------------------------------------

class TestCustomCheckDecorator:
    def test_decorator_populates_decorated_checks(self):
        @custom_check("_test_decorator_check")
        def my_fn(context) -> pd.Series:
            return pd.Series([True])

        assert "_test_decorator_check" in DECORATED_CHECKS
        func, kind = DECORATED_CHECKS["_test_decorator_check"]
        assert func is my_fn
        assert kind == "check"

    def test_decorated_function_still_callable(self):
        @custom_check("_test_callable_check")
        def my_fn(context) -> pd.Series:
            df = context["df"]
            return pd.Series([True] * len(df), index=df.index)

        df = pd.DataFrame({"col": [1, 2]})
        result = my_fn({"df": df})
        assert isinstance(result, pd.Series)
        assert result.all()


# ---------------------------------------------------------------------------
# load_custom_checks_module
# ---------------------------------------------------------------------------

class TestLoadCustomChecksModule:
    def test_registers_decorated_function(self, tmp_path, check_registry):
        module_path = tmp_path / "my_checks.py"
        _write_module(module_path, (
            "from proto_pipe.checks.helpers import custom_check\n"
            "import pandas as pd\n"
            "\n"
            "@custom_check('margin_check')\n"
            "def check_margin(context, col: str = 'margin') -> pd.Series:\n"
            "    df = context['df']\n"
            "    return df[col] >= 0.2\n"
        ))
        load_custom_checks_module(str(module_path), check_registry)

        assert "margin_check" in check_registry.available()

    def test_custom_check_is_runnable(self, tmp_path, check_registry):
        module_path = tmp_path / "my_checks.py"
        _write_module(module_path, (
            "from proto_pipe.checks.helpers import custom_check\n"
            "import pandas as pd\n"
            "\n"
            "@custom_check('pct_check')\n"
            "def check_pct(context, col: str = 'pct') -> pd.Series:\n"
            "    df = context['df']\n"
            "    return df[col] <= 1.0\n"
        ))
        from proto_pipe.checks.helpers import load_custom_checks_module
        load_custom_checks_module(str(module_path), check_registry)

        df = pd.DataFrame({"pct": [0.5, 1.5, 0.9]})
        from proto_pipe.checks.result import CheckResult
        result = check_registry.run("pct_check", {"df": df})
        assert isinstance(result, CheckResult)
        assert result.passed is False
        assert result.mask.sum() == 1  # 1 row fails (1.5 > 1.0)

    def test_missing_module_exits(self, tmp_path, check_registry):
        from proto_pipe.checks.helpers import load_custom_checks_module
        with pytest.raises(SystemExit):
            load_custom_checks_module(str(tmp_path / "nonexistent.py"), check_registry)

    def test_module_with_syntax_error_exits(self, tmp_path, check_registry):
        module_path = tmp_path / "bad_checks.py"
        _write_module(module_path, "def broken(:\n    pass\n")
        from proto_pipe.checks.helpers import load_custom_checks_module
        with pytest.raises(SystemExit):
            load_custom_checks_module(str(module_path), check_registry)

    def test_module_with_no_decorated_functions_warns(
            self, tmp_path, check_registry, capsys
    ):
        module_path = tmp_path / "empty_checks.py"
        _write_module(module_path, "# no decorated functions here\nX = 1\n")

        original = dict(DECORATED_CHECKS)
        DECORATED_CHECKS.clear()

        from proto_pipe.checks.helpers import load_custom_checks_module
        try:
            load_custom_checks_module(str(module_path), check_registry)
        finally:
            DECORATED_CHECKS.update(original)

        captured = capsys.readouterr()
        assert "warn" in captured.out.lower() or "no" in captured.out.lower()


# ---------------------------------------------------------------------------
# End-to-end: custom check via register_from_config
# ---------------------------------------------------------------------------

class TestCustomCheckEndToEnd:
    def test_custom_check_runs_via_config(self, tmp_path, check_registry, report_registry):
        module_path = tmp_path / "e2e_checks.py"
        _write_module(module_path, (
            "from proto_pipe.checks.helpers import custom_check\n"
            "import pandas as pd\n"
            "\n"
            "@custom_check('e2e_custom_check')\n"
            "def check_e2e(context) -> pd.Series:\n"
            "    df = context['df']\n"
            "    return pd.Series([True] * len(df), index=df.index)\n"
        ))
        from proto_pipe.io.registry import (
            register_from_config,
        )
        from proto_pipe.checks.helpers import load_custom_checks_module
        load_custom_checks_module(str(module_path), check_registry)

        config = {
            "templates": {},
            "reports": [
                {
                    "name": "e2e_report",
                    "source": {
                        "type": "duckdb",
                        "path": str(tmp_path / "fake.db"),
                        "table": "sales",
                    },
                    "options": {},
                    "checks": [{"name": "e2e_custom_check"}],
                }
            ],
        }
        register_from_config(config, check_registry, report_registry)

        df = pd.DataFrame({"col": [1, 2, 3]})
        from proto_pipe.checks.result import CheckResult
        result = check_registry.run("e2e_custom_check", {"df": df})
        assert isinstance(result, CheckResult)
        assert result.passed is True

# ---------------------------------------------------------------------------
# CLAUDE.md behavioral guarantee tests
# ---------------------------------------------------------------------------

class TestValidateCheckGateAppliesOnModuleLoad:
    """validate_check is the single validation gate — applies even via module load.

    CLAUDE.md guarantee:
      'validate_check in checks/registry.py is the single validation gate —
       checks annotation, kind, return type.
       CheckRegistry._checks holds only vetted contracts.'
    """

    def test_bad_annotation_in_module_not_registered(self, tmp_path, check_registry):
        """A function with no return annotation loaded from a module must not
        be registered into _checks, even though @custom_check was applied."""
        module_path = tmp_path / "bad_annotation_checks.py"
        _write_module(module_path,
            "from proto_pipe.checks.helpers import custom_check\n"
            "\n"
            "@custom_check('bad_annotation_check', kind='check')\n"
            "def bad_fn(context):  # no return annotation\n"
            "    return context['df']\n"
        )

        load_custom_checks_module(str(module_path), check_registry)

        assert "bad_annotation_check" not in check_registry._checks, (
            "validate_check gate must reject bad-annotation functions even when "
            "loaded from a module file"
        )
        assert "bad_annotation_check" in check_registry._bad_checks, (
            "Rejected check must appear in _bad_checks with a reason"
        )


class TestBuiltInChecksPopulatedAfterLoad:
    """load_custom_checks_module adds to BUILT_IN_CHECKS, not just the registry.

    CLAUDE.md guarantee:
      'load_custom_checks_module imports the user module, iterates
       _DECORATED_CHECKS, calls register_custom_check.'
    register_custom_check is documented to add to BUILT_IN_CHECKS so that
    the check persists across registry instances.
    """

    def test_built_in_checks_populated_after_module_load(self, tmp_path, check_registry):
        from proto_pipe.checks.built_in import BUILT_IN_CHECKS

        module_path = tmp_path / "persistent_checks.py"
        _write_module(module_path,
            "from proto_pipe.checks.helpers import custom_check\n"
            "import pandas as pd\n"
            "\n"
            "@custom_check('persistent_check')\n"
            "def my_check(context, col: str = 'price') -> pd.Series:\n"
            "    return context['df'][col] >= 0\n"
        )

        original_keys = set(BUILT_IN_CHECKS.keys())
        load_custom_checks_module(str(module_path), check_registry)

        assert "persistent_check" in BUILT_IN_CHECKS, (
            "load_custom_checks_module must add the check to BUILT_IN_CHECKS, "
            "not just the registry instance, so it persists across registry instances"
        )

        # Cleanup
        BUILT_IN_CHECKS.pop("persistent_check", None)
