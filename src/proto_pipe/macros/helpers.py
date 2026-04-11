"""Helper for registering custom Python macros.

Mirrors checks/helpers.py pattern: @macro decorator stages functions
in DECORATED_MACROS; load_macros_module imports the user module and
registers each into MacroRegistry via validate_macro.
"""
from __future__ import annotations

import importlib.util
import sys

from pathlib import Path
from typing import Callable

from proto_pipe.macros.registry import MacroRegistry, validate_macro, MacroContract

# Staging area for functions decorated with @macro.
# Maps macro name → (func,). Not validated yet — validation happens
# when load_macros_module calls validate_macro() for each entry.
DECORATED_MACROS: dict[str, tuple[Callable]] = {}


def macro(name: str):
    """Decorator to mark a function as a Python macro.

    The decorated function will be automatically registered when the pipeline
    loads the module specified by `custom_macros_module` in pipeline.yaml.

    Args:
        name: The macro name used in SQL queries.

    Example:
        @macro("apply_quota_share")
        def apply_quota_share(premium: float, share: float) -> float:
            return premium * share
    """
    def decorator(func: Callable) -> Callable:
        DECORATED_MACROS[name] = (func,)
        return func
    return decorator


def load_macros_module(
    module_path: str,
    macro_registry: MacroRegistry,
) -> None:
    """Import a user-supplied Python module and register any @macro functions.

    Called automatically at startup when `custom_macros_module` is set
    in pipeline.yaml. Safe to call multiple times — already-registered
    names are overwritten with the latest definition.

    Args:
        module_path:    Path to the .py file, relative to the working directory.
        macro_registry: The MacroRegistry instance to register macros into.

    Raises:
        SystemExit: If the module file is not found or raises an import error.
    """
    path = Path(module_path)
    if not path.exists():
        print(
            f"\n[error] custom_macros_module: '{module_path}' not found.\n"
            f"Check the path in pipeline.yaml and try again."
        )
        sys.exit(1)

    spec = importlib.util.spec_from_file_location("_custom_macros", path)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        print(
            f"\n[error] Failed to import custom_macros_module '{module_path}':\n"
            f"{e}"
        )
        sys.exit(1)

    if not DECORATED_MACROS:
        print(
            f"[warn] Loaded '{module_path}' but found no @macro decorated functions."
        )
        return

    for name, (func,) in DECORATED_MACROS.items():
        try:
            contract = validate_macro(func)
            # Override name from decorator (validate_macro uses __name__)
            contract = MacroContract(
                name=name,
                params=contract.params,
                param_types=contract.param_types,
                return_type=contract.return_type,
                source="python",
                func=func,
                func_name=contract.func_name,
            )
            macro_registry.register(contract)
            print(f"[macro] Registered '{name}' from '{module_path}'")
        except TypeError as e:
            print(f"[macro-fail] '{name}' in '{module_path}': {e}")
