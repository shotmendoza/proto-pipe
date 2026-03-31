"""
Helper for registering custom checks.
Keeps CheckRegistry and BUILT_IN_CHECKS in sync so custom checks
work both programmatically and when referenced by name in the config.

Custom checks can also be auto-loaded from a module path declared in
pipeline.yaml via the `custom_checks_module` key. Any function in that
module decorated with @custom_check will be registered automatically
when the pipeline starts.

kind="check" — returns pd.Series[bool], failures written to validation_flags
kind="transform" — returns pd.Series or pd.DataFrame, result written back to the
                   report table in DuckDB after all checks have run
"""

from functools import partial
from typing import Callable, Literal

from proto_pipe.checks.built_in import BUILT_IN_CHECKS
from proto_pipe.registry.base import CheckRegistry

# Staging area for functions decorated with @custom_check.
# Maps check name -> (func, kind). Not validated yet — validation happens
# when load_custom_checks_module calls check_registry.register() for each entry.
_DECORATED_CHECKS: dict[str, tuple[Callable, str]] = {}


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
                            Failures are written to validation_flags.
              "transform" — function returns pd.Series or pd.DataFrame.
                            The result is written back to the report table in DuckDB
                            after all checks have run. Runs in config order.
                            On exception, the original table is left unchanged
                            and the error is reported.
        Examples:

        @custom_check("margin_check")
        def check_margin(context: dict, col: str = "margin", threshold: float = 0.2) -> pd.Series:
            df = context["df"]
            return df[col] >= threshold

        @custom_check("normalize_transaction_type", kind="transform")
        def transform_transaction_type(context: dict, col: str = "transaction_type") -> pd.Series:
            df = context["df"]
            result = df[col].replace({"Issuance": "Reinstatement"})
            result.name = col
            return result
    """
    if kind not in ("check", "transform"):
        raise ValueError(f"kind must be 'check' or 'transform', got '{kind}'")

    def decorator(func: Callable) -> Callable:
        _DECORATED_CHECKS[name] = (func, kind)
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
