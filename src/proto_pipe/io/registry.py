"""Config loader.

Reads reports_config.yaml, resolves templates, and registers
all checks as partials on the registry so the runner never
needs to know about individual check params.
"""
import importlib.util
import sys
import uuid
from functools import partial
from os import PathLike
from pathlib import Path
from typing import Literal

import click
import pandas as pd
from ruamel.yaml import YAML

from proto_pipe.checks.built_in import BUILT_IN_CHECKS
from proto_pipe.registry.base import CheckRegistry, ReportRegistry


def load_config(config_path: str | PathLike) -> dict:
    """loads and returns a YAML config file, based on the given path

    :param config_path: on the path to the config file you want to load
    :return: the config dict
    """
    with open(config_path) as f:
        yaml = YAML()
        return yaml.load(f) or {}


def write_config(config: dict, config_path: str | PathLike) -> None:
    """Writes a YAML config file, based on the given path and config dict"""
    with open(config_path, "w") as f:
        yaml = YAML()
        yaml.dump(config, f)


def _build_check_keys(
        func_name: str,
        params: dict
) -> str:
    """Generates a deterministic unique key based on the function name and its sorted
    parameters.

    This function creates a UUID using the DNS namespace and a concatenation of
    the provided function name with the sorted dictionary of parameters. It ensures
    reproducibility by producing the same output for identical inputs.

    :param func_name: The name of the function for which the key will be generated.
    :param params: A dictionary representing the parameters to be considered when generating the key.
    :return: A unique string key generated based on the function name and parameters.
    """
    # deterministic — same inputs always produce the same UUID
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{func_name}:{sorted(params.items())}"))


def resolve_check(
        check: dict,
        templates: dict
) -> dict:
    """Resolve the given check dictionary using predefined templates. If the check
    dictionary contains a "template" key, this function looks up the corresponding
    template in the provided templates dictionary, copies its contents, and merges it
    with the check dictionary.

    Any parameters within the template are also deep-copied to avoid unintentional mutations.

    :param check: The dictionary representing a check that may include a reference to a template by its "template" key.
    :param templates: A dictionary mapping template names to their corresponding template definitions.
        Each template should be a dictionary.
    :return: A dictionary representing the resolved check, which includes data
        copied and merged from the referenced template, if applicable.
    :raises ValueError: If the "template" key in the check dictionary refers to a template name
        that is not found in the templates dictionary.
    """
    if "template" not in check:
        return check
    name = check["template"]
    if name not in templates:
        raise ValueError(f"Unknown template '{name}'")
    resolved = templates[name].copy()
    resolved["params"] = {
        **resolved.get("params", {}),
        **check.get("params", {}),  # check-level param override template params
    }
    return resolved


def _build_alias_param_map(alias_map: list[dict]) -> dict[str, list[str]]:
    """Build {param_name: [col1, col2, ...]} from alias_map list.

    :param alias_map: list of {param, column} dicts from report config
    :return: dict mapping param name to list of columns
    """
    result: dict[str, list[str]] = {}
    for entry in alias_map:
        result.setdefault(entry["param"], []).append(entry["column"])
    return result


def _expand_check_with_alias_map(
    func_name: str,
    params: dict,
    alias_param_map: dict[str, list[str]],
    check_registry: CheckRegistry,
) -> list[str]:
    """Expand a check into N registered check names using alias_map column params.

    For each column param that appears in alias_param_map:
    - If one column: register one check with that column baked in.
    - If N columns:  register N checks, one per column. Scalar params broadcast.

    Returns list of registered check names (length 1 for single-column or no
    alias params, length N for multi-column params).

    :param func_name: name of the function in BUILT_IN_CHECKS
    :param params: scalar params from the check config (column params excluded)
    :param alias_param_map: {param: [col1, col2, ...]} built from report alias_map
    :param check_registry: registry to register expanded checks into
    :return: list of registered check names
    """

    from proto_pipe.checks.inspector import CheckParamInspector

    func = BUILT_IN_CHECKS.get(func_name)
    if func is None:
        raise ValueError(f"No built-in check named '{func_name}'")

    inspector = CheckParamInspector(func)
    col_params = inspector.column_params()

    # Which of this check's column params are covered by alias_map?
    alias_col_params = {
        p: alias_param_map[p] for p in col_params if p in alias_param_map
    }

    if not alias_col_params:
        # No alias expansion — register normally with whatever params are given
        check_name = _build_check_keys(func_name, params)
        if check_name not in check_registry.available():
            _register_check(check_name, func_name, params, check_registry)
        return [check_name]

    # Validate that all column params with multiple entries have the same length.
    # Single-entry params (len == 1) are exempt — they broadcast silently.
    lengths = {p: len(cols) for p, cols in alias_col_params.items()}
    multi_lengths = {p: l for p, l in lengths.items() if l > 1}
    unique_lengths = set(multi_lengths.values())
    if len(unique_lengths) > 1:
        detail = ", ".join(f"'{p}': {l}" for p, l in multi_lengths.items())
        raise ValueError(
            f"alias_map column params for '{func_name}' have unequal lengths: {detail}. "
            f"All multi-column params must have the same number of entries, "
            f"or exactly one entry (which broadcasts across all runs)."
        )

    max_len = max(lengths.values())
    names = []
    for i in range(max_len):
        run_params = dict(params)  # start with scalar params
        for p, cols in alias_col_params.items():
            # len==1 params broadcast; all others are guaranteed equal length above
            run_params[p] = cols[i] if i < len(cols) else cols[0]
        check_name = _build_check_keys(func_name, run_params)
        if check_name not in check_registry.available():
            _register_check(check_name, func_name, run_params, check_registry)
        names.append(check_name)
    return names


def resolve_check_uuid(
        report: dict,
        check_registry: CheckRegistry,
        alias_param_map: dict[str, list[str]] | None = None,
) -> list[str]:
    """Resolve check UUIDs for a report, expanding alias_map column params.

    For checks whose column params appear in alias_param_map, registers N
    expanded versions (one per column) and returns all their names.

    :param report: Report config dict with a "checks" list.
    :param check_registry: Registry to register resolved checks into.
    :param alias_param_map: {param: [col1, col2]} built from report alias_map.
                            Pass None or {} if report has no alias_map.
    :return: List of resolved check names.
    """
    alias_param_map = alias_param_map or {}
    resolved_check_names: list[str] = []

    for check in report.get("checks", []):
        template_name = check.get("template")
        if template_name is not None:
            resolved_check_names.append(template_name)
            continue

        func_name = check["name"]
        params = check.get("params", {}) or {}

        if alias_param_map:
            expanded = _expand_check_with_alias_map(
                func_name, params, alias_param_map, check_registry
            )
            resolved_check_names.extend(expanded)
        else:
            check_name = _build_check_keys(func_name, params)
            if check_name not in check_registry.available():
                _register_check(check_name, func_name, params, check_registry)
            resolved_check_names.append(check_name)

    return resolved_check_names


def _register_check(
        name: str,
        func_name: str,
        params: dict,
        check_registry: CheckRegistry,
        kind: Literal["check", "transform"] = "check",
) -> None:
    """Register a check function into the check registry.

    Looks up func_name in BUILT_IN_CHECKS and registers it, optionally
    baking in params via partial.

    :param name:           Unique key for the registry.
    :param func_name:      Name of the function in BUILT_IN_CHECKS.
    :param params:         Params to bake in via partial, if any.
    :param check_registry: The registry to register into.
    :param kind:           "check" or "transform".
    :raises ValueError: If func_name is not in BUILT_IN_CHECKS.
    """
    import inspect as _inspect

    func = BUILT_IN_CHECKS.get(func_name)
    if func is None:
        raise ValueError(f"No built-in check named '{func_name}'")

    filled: dict = {}
    if params:
        sig = _inspect.signature(func)
        for k, v in params.items():
            if v is None:
                param = sig.parameters.get(k)
                if param and param.default is not _inspect.Parameter.empty:
                    filled[k] = param.default
                    print(
                        f"[warn] '{func_name}' param '{k}' is None in config"
                        f" — using default value: {param.default!r}"
                    )
                else:
                    print(
                        f"[warn] '{func_name}' param '{k}' is None in config"
                        f" and has no default — param skipped, check may fail"
                        f" at runtime. Edit reports_config.yaml to set a value."
                    )
            else:
                filled[k] = v

    if filled:
        check_registry.register(name, partial(func, **filled), kind=kind)
    else:
        check_registry.register(name, func, kind=kind)


def register_from_config(
    config: dict,
    check_registry: CheckRegistry,
    report_registry: ReportRegistry,
) -> None:
    """Register checks and reports from a given configuration dictionary.

    This function processes a configuration dictionary to register both named
    checks (templates) and reports into provided registries.

    Templates are first registered as named checks with pre-baked parameters. Reports are then
    processed to identify their associated checks, resolving any templates,
    and registering inline checks if they are not already available.

    :param config: Configuration data containing information about templates and reports to be registered.
    :param check_registry: Registry instance used for registering and managing checks.
    :param report_registry: Registry instance used for registering and managing reports.
    :return: None
    """
    templates = config.get("templates", {})

    # Register templates as named checks (params baked in)
    for template_name, template in templates.items():
        _register_check(
            name=template_name,
            func_name=template["name"],
            params=template.get("params", {}),
            check_registry=check_registry,
        )

    # Register reports and any inline checks they define
    for report in config.get("reports", []):
        alias_map = report.get("alias_map", [])
        alias_param_map = _build_alias_param_map(alias_map) if alias_map else {}

        # Store the report with its resolved check name list
        resolved_check_names = resolve_check_uuid(report, check_registry, alias_param_map)

        report_registry.register(
            report["name"],
            {
                **report,
                "resolved_checks": resolved_check_names,
            },
        )


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
    from proto_pipe.checks.helpers import (
        _DECORATED_CHECKS,
        register_custom_check,
    )

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

    if not _DECORATED_CHECKS:
        print(
            f"[warn] Loaded '{module_path}' but found no @custom_check decorated functions."
        )
        return

    for name, (func, kind) in _DECORATED_CHECKS.items():
        register_custom_check(name, func, check_registry, kind=kind)
        print(f"[custom_check] Registered '{name}' (kind={kind}) from '{module_path}'")


# ---------------------------------------------------------------------------
# Filename resolution
# ---------------------------------------------------------------------------
def resolve_filename(template: str, report_name: str, run_date: str) -> str:
    """Substitute {report_name} and {date} placeholders in filename templates."""
    return template.replace("{report_name}", report_name).replace("{date}", run_date)


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------
def write_csv(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


def write_xlsx_sheet(dfs: dict[str, pd.DataFrame], output_path: Path) -> None:
    """Write multiple DataFrames as sheets into a single Excel file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df in dfs.items():
            df = df.copy()
            for col in df.select_dtypes(include=["datetimetz"]).columns:
                df[col] = df[col].dt.tz_localize(None)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
