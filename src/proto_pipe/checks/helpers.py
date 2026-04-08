"""
Helper for registering custom checks.
Keeps CheckRegistry and BUILT_IN_CHECKS in sync so custom checks
work both programmatically and when referenced by name in the config.

Custom checks can also be auto-loaded from a module path declared in
pipeline.yaml via the `custom_checks_module` key. Any function in that
module decorated with @custom_check will be registered automatically
when the pipeline starts.

kind="check" — returns pd.Series[bool], failures written to validation_block
kind="transform" — returns pd.Series or pd.DataFrame, result written back to the
                   report table in DuckDB after all checks have run
"""
from __future__ import annotations

import importlib.util
import sys

from functools import partial
from pathlib import Path
from typing import Callable, Literal

from proto_pipe.checks.built_in import BUILT_IN_CHECKS
from proto_pipe.checks.registry import CheckRegistry
from proto_pipe.io.config import config_settings

# Staging area for functions decorated with @custom_check.
# Maps check name -> (func, kind). Not validated yet — validation happens
# when load_custom_checks_module calls check_registry.register() for each entry.
DECORATED_CHECKS: dict[str, tuple[Callable, str]] = {}


def custom_check(
        name: str,
        kind: Literal["check", "transform"] = "check"
):
    """
    Decorator to mark a function as a custom check.

    The decorated function will be automatically registered when the pipeline
    loads the module specified by `custom_checks_module` in pipeline.yaml.

    Args:
        name: The check name used in reports_config.yaml.

        kind: "check" (default) or "transform".
              "check" — function returns pd.Series[bool]; True = row passes.
                            Failures are written to validation_block.
              "transform" — function returns pd.Series or pd.DataFrame.
                            The result is written back to the report table in DuckDB
                            after all checks have run. Runs in config order.
                            On exception, the original table is left unchanged
                            and the error is reported.
        Examples:

        @custom_check("margin_check")
        def check_margin(col: pd.Series, threshold: float = 0.2) -> pd.Series[bool]:
            return col >= threshold

        @custom_check("normalize_transaction_type", kind="transform")
        def transform_transaction_type(col: pd.Series) -> pd.Series:
            result = col.replace({"Issuance": "Reinstatement"})
            result.name = col.name
            return result

        # With full table access (multiple columns needed):
        @custom_check("check_ratio", kind="check")
        def check_ratio(df: pd.DataFrame, numerator: str, denominator: str) -> pd.Series[bool]:
            return df[numerator] / df[denominator] >= 0.5

        # Scalar check — function called per row, pipeline assembles the Series:
        @custom_check("check_region_code")
        def check_region_code(region: str) -> bool:
            return region in ("EMEA", "APAC", "AMER")
    """
    if kind not in ("check", "transform"):
        raise ValueError(f"kind must be 'check' or 'transform', got '{kind}'")

    def decorator(func: Callable) -> Callable:
        DECORATED_CHECKS[name] = (func, kind)
        return func

    return decorator


def register_custom_check(
        name: str,
        func: Callable,
        check_registry: CheckRegistry,
        kind: Literal["check", "transform"] = "check",
        **default_params,
) -> None:
    """Register a custom check so it's available for the end user.

    This will allow the checks to be referenced:
        - In the check registry (for direct use by the runner)
        - In BUILT_IN_CHECKS (so the config loader can find it by name)

    :param kind: Classified as a "check" or "transform". Passed through to the registry.
    :param name: The name to register under (used in YAML config too).
    :param func: The check function -> func(context, **params) -> dict.
    :param check_registry: The CheckRegistry instance to register against.
    :param default_params: Optional params to bake in via partial (on the function call)
    """
    BUILT_IN_CHECKS[name] = func
    if default_params:
        check_registry.register(name, partial(func, **default_params), kind=kind)
    else:
        check_registry.register(name, func, kind=kind)


def load_custom_checks(check_registry: CheckRegistry) -> None:
    """Loads custom checks into the given check registry by importing a custom checks
    module defined in the configuration settings. If no custom checks module is
    specified in the configuration settings, this function performs no action.

    :param check_registry: The registry where custom checks will be loaded.
    :return: None
    """

    module_path = config_settings().get("custom_checks_module")
    if module_path:
        load_custom_checks_module(module_path, check_registry)


def load_custom_checks_module(
    module_path: str,
    check_registry: CheckRegistry,
) -> None:
    """
    Import a user-supplied Python module and register any functions decorated
    with @custom_check into both BUILT_IN_CHECKS and the check registry.

    Called automatically at CLI startup when `custom_checks_module` is set
    in pipeline.yaml. Safe to call multiple times — already-registered names
    are overwritten with the latest definition.

    Args:
        module_path:    Path to the .py file, relative to the working directory.
        check_registry: The CheckRegistry instance to register checks into.

    Raises:
        SystemExit: If the module file is not found or raises an import error,
                    a clear message is printed and the pipeline exits rather than
                    proceeding with missing checks.
    """
    path = Path(module_path)
    if not path.exists():
        print(
            f"\n[error] custom_checks_module: '{module_path}' not found.\n"
            f"Check the path in pipeline.yaml and try again."
        )
        sys.exit(1)

    # Load the module from its file path without requiring it to be installed
    spec = importlib.util.spec_from_file_location("_custom_checks", path)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        print(
            f"\n[error] Failed to import custom_checks_module '{module_path}':\n"
            f"{e}"
        )
        sys.exit(1)

    if not DECORATED_CHECKS:
        print(
            f"[warn] Loaded '{module_path}' but found no @custom_check decorated functions."
        )
        return

    for name, (func, kind) in DECORATED_CHECKS.items():
        register_custom_check(name, func, check_registry, kind=kind)
        print(f"[custom_check] Registered '{name}' (kind={kind}) from '{module_path}'")
