"""
Helper for registering custom checks.
Keeps CheckRegistry and BUILT_IN_CHECKS in sync so custom checks
work both programmatically and when referenced by name in the config.

Custom checks can also be auto-loaded from a module path declared in
pipeline.yaml via the `custom_checks_module` key. Any function in that
module decorated with @custom_check will be registered automatically
when the pipeline starts.
"""

from functools import partial
from typing import Callable

from proto_pipe.checks.built_in import BUILT_IN_CHECKS
from proto_pipe.registry.base import CheckRegistry

# Registry of functions decorated with @custom_check, populated at import time.
# Maps check name -> function. Consumed by load_custom_checks_module() in registry.py.
_DECORATED_CHECKS: dict[str, Callable] = {}


def custom_check(name: str):
    """
    Decorator to mark a function as a custom check.

    The decorated function will be automatically registered when the pipeline
    loads the module specified by `custom_checks_module` in pipeline.yaml.

    Args:
        name: The check name used in reports_config.yaml.

    Example:

        @custom_check("margin_check")
        def check_margin(context, col: str) -> pd.Series:
            return context["df"][col] > threshold
    """
    def decorator(func: Callable) -> Callable:
        from proto_pipe.checks.inspector import CheckParamInspector

        inspector = CheckParamInspector(func)
        if not inspector.returns_boolean_series():
            print(
                f"[warn] Custom check '{name}' has no valid return annotation.\n"
                f"Expected: -> pd.Series\n"
                f"The check will not be available until this is fixed.\n"
                f"Example: def {func.__name__}(context, col: str) -> pd.Series: ..."
            )
        _DECORATED_CHECKS[name] = func
        return func
    return decorator


def register_custom_check(
    name: str,
    func: Callable,
    check_registry: CheckRegistry,
    **default_params,
) -> None:

    """Register a custom check so it's available for the end user.

    This will allow the checks to be referenced:
        - In the check registry (for direct use by the runner)
        - In BUILT_IN_CHECKS (so the config loader can find it by name)

    :param name: The name to register under (used in YAML config too).
    :param func: The check function -> func(context, **params) -> dict.
    :param check_registry: The CheckRegistry instance to register against.
    :param default_params: Optional params to bake in via partial (on the function call)
    """
    BUILT_IN_CHECKS[name] = func
    if default_params:
        check_registry.register(name, partial(func, **default_params))
    else:
        check_registry.register(name, func)
