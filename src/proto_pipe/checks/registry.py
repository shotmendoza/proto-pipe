from __future__ import annotations

import functools
import hashlib
import inspect

from dataclasses import dataclass, field
from functools import partial
from typing import Callable

import duckdb
import pandas as pd


# ---------------------------------------------------------------
# Check Registry
#
# The idea behind the registry is that you "registry" functions
# that you want to run all together.
#
# The strength of the registry is that you can use an arbitrary
# parameter set, and they would all run in the same way. This
# makes it so whatever object is running the functions, do not
# need to be aware of the shape of the functions, thus, decoupling
# the tie between an object needing to know the shape, and the
# object running it.
#
# The CheckRegistry, thus, registers any function with any param
# shape, and will be able to handle the user running the different
# functions.
# ----------------------------------------------------------------
# TODO: anything that is inspecting, should be inside CheckInspect
from proto_pipe.shared import is_str_annotation, is_series_annotation, is_dataframe_annotation


class CheckRegistry:
    """One of the main parts of `proto-pipe`. This object is used as a source of truth
    for all templates and checks to *registry* into, so that the checks are applied and run.

    """
    def __init__(self):
        self._checks: dict[str, CheckContract] = {}
        """Maps name -> CheckContract. Only vetted functions enter the registry"""

        self._bad_checks: dict[str, str] = {}
        """Maps name -> Failure Reason. Checks that failed validate_check land here. 
        Populated whenever registration is attempted and fails. Queryable via failed()."""

    def get(self, name: str) -> Callable | None:
        """Return the registered function for a check name, or None if not found."""
        entry = self._checks.get(name)
        return entry.func if entry else None

    def get_kind(self, name: str) -> str:
        """Return the kind ('check' or 'transform') for a registered name.

        :raises ValueError: if name is not registered
        """
        if name not in self._checks:
            raise ValueError(f"No check registered under '{name}'")
        return self._checks[name].kind

    def get_contract(self, name: str) -> "CheckContract":
        """Return the full CheckContract for a registered name.

        The contract carries the inspection flags set at registration time
        (needs_dataframe, needs_series, is_scalar, is_legacy). Use this
        instead of re-running CheckParamInspector at call time.

        Callers must use this method — never access _checks directly.

        :raises ValueError: if name is not registered
        """
        if name not in self._checks:
            raise ValueError(f"No check registered under '{name}'")
        return self._checks[name]

    def failed(self) -> dict[str, str]:
        """Return {name: failure_reason} for all checks that failed registration.

        Use vp check-func for a structured view of these failures with fix suggestions.
        """
        return dict(self._bad_checks)

    def register(
            self,
            name: str,
            func: Callable,
            kind: str = "check"
    ) -> None:
        """Register a function into the registry.

        Routes the CheckAudit returned by validate_check: passed audits go to
        _checks, failed audits go to _bad_checks. Either way the outcome is
        stored — run vp check-func to see the full picture.

        :param name: name of the function, used as a reference.
        :param func: function to be registered.
        :param kind: 'check' (default) or 'transform'.
        """
        audit = validate_check(name, func, kind)
        if audit.passed:
            self._checks[name] = audit.contract
            return
        self._bad_checks[name] = audit.failure_reason

    def register_with_params(
            self,
            name: str,
            func: Callable,
            kind: str = "check",
            **params
    ) -> None:
        """
        Registers a function with additional parameters. The registration associates the function
        with a name and a kind, and partializes the function with the provided parameters.

        :param name: The name to associate with the function upon registration.
        :type name: str
        :param func: The function to be registered. It is the callable that will be
            associated with the given name and kind.
        :type func: Callable
        :param kind: Specifies the kind of function being registered. Defaults to "check".
            This parameter categorizes the type of function.
        :type kind: str
        :param params: Arbitrary keyword arguments to be passed to the function as additional
            parameters during its partialization.
        :type params: dict
        :return: This function does not return a value.
        :rtype: None
        """
        self.register(name, partial(func, **params), kind=kind)

    def run(
            self,
            name: str,
            context: dict
    ) -> Callable:
        """Run a specific check or transform in the registry.

        :param name: name of the function, used as a reference
        :param context: the parameters to pass to the function
        """
        if name not in self._checks:
            raise ValueError(f"No check registered under '{name}'")
        return self._checks[name].func(context)

    def available(self) -> list[str]:
        """Return a list of all registered names (checks and transforms).

        :return: available check/transform names
        """
        return list(self._checks.keys())

    def checks_only(self) -> list[str]:
        """Return names of registered entries with kind='check'."""
        return [n for n, cr in self._checks.items() if cr.kind == "check"]

    def transforms_only(self) -> list[str]:
        """Return names of registered entries with kind='transform'."""
        return [n for n, cr in self._checks.items() if cr.kind == "transform"]


class ReportRegistry:
    """Stores report definitions (name, source config, check names, options).
    Knows nothing about check implementations — references checks by name only.
    The runner mediates between ReportRegistry and CheckRegistry.
    """

    def __init__(self):
        self._reports: dict[str, dict] = {}
        """The reports that have been registered. Key-Value pair of report name, report config"""

    def get_or_none(self, name: str) -> dict | None:
        """Returns the report configuration for a given name, or None if not found."""
        return self._reports.get(name)

    def register(self, name: str, report_config: dict) -> None:
        """Registers a new report configuration.

        The method associates a report name with its corresponding configuration.
        If the report name already exists, the old configuration is overwritten.

        :param name: The unique identifier for the report.
        :param report_config: A dictionary containing the configuration details for the report.
        :return: None
        """
        self._reports[name] = report_config

    def get(self, name: str) -> dict:
        """Fetches a report from the list of available reports.

        Raises an exception if the specified report name is not found in the
        registered reports.

        :param name: The name of the report to fetch.
        :return: A dictionary containing the details of the requested report.
        :raises ValueError: If no report is registered under the provided name.
        """
        if name not in self._reports:
            raise ValueError(f"No report registered under '{name}'")
        return self._reports[name]

    def all(self) -> list[dict]:
        """Provides a method to retrieve all report data stored in the instance.

        :return: A list of dictionary objects representing the reports
        :rtype: list[dict]
        """
        return list(self._reports.values())

    def available(self) -> list[str]:
        """Returns a list of available report names.

        This method retrieves all the keys from the `_reports` dictionary and
        returns them as a list. Each key represents the name of a report that
        is available.

        :return: List of strings representing the names of available reports.
        """
        return list(self._reports.keys())


# Global registry instance
check_registry = CheckRegistry()
report_registry = ReportRegistry()


@dataclass
class CheckContract:
    """A vetted check or transform function, ready for the CheckRegistry.

    Created exclusively by validate_check() after passing all validation.
    CheckRegistry stores and communicates through CheckContract objects —
    anything in the registry has been signed off by CheckParamInspector.

    Inspection results are stored here once at registration time so the runner
    never needs to re-inspect a function. Use CheckRegistry.get_contract(name)
    to access these flags — never access _checks directly.

    Attributes:
        func:               The callable. For kind='check', wrapped with
                            wrap_series_check so it returns CheckResult.
        kind:               'check' or 'transform'.
        needs_dataframe:    True if the function has a pd.DataFrame param.
                            Requires a pandas roundtrip — pending_df must be loaded.
        needs_series:       True if the function has pd.Series param(s) and no
                            pd.DataFrame param. Can extract columns from DuckDB.
        is_scalar:          True if the function has no pd.Series or pd.DataFrame
                            params and no legacy context param. Per-row apply via
                            pandas or DuckDB UDF.
        is_legacy:          True if the function uses context: dict or a positional
                            context param (legacy calling convention). Always routed
                            to the pandas path — requires context["df"].
        series_columns:     Column names baked into Series params at registration
                            time. Used by _compute_report for bulk column pre-fetch.
        func_name:          Original function __name__, resolved once at
                            validate_check time. Used by display_name() in prompts.py.
        col_backed_params:  {param_name: column_name} for all Series params baked
                            at registration time. Used by the pandas transform
                            write-back path to resolve the _output column.
    """

    func: Callable
    kind: str = "check"
    needs_dataframe: bool = False
    needs_series: bool = False
    is_scalar: bool = False
    is_legacy: bool = False
    series_columns: list = field(default_factory=list)
    # Column names baked into Series params at registration time.
    # Populated on the Series path so _compute_report can pre-fetch
    # all needed columns in one bulk query — never re-derived at call time.
    func_name: str = ""
    # Original function __name__, resolved once at validate_check time.
    # Used by display_name() in prompts.py — never re-inspected at call time.
    col_backed_params: dict = field(default_factory=dict)
    # {param_name: column_name} for all Series params baked at registration time.
    # Used by the pandas transform write-back path in runner.py to resolve the
    # _output column via _resolve_output_col — eliminates runtime inspection.


@dataclass
class CheckAudit:
    """The result of attempting to validate a check or transform function.

    Returned by validate_check() in all cases — success or failure.
    CheckRegistry uses this to route: passed audits go to _checks,
    failed audits go to _bad_checks.

    Attributes:
        contract: The validated CheckContract, or None if validation failed.
        failure_reason: Plain-English explanation of why validation failed.
                        None when the audit passed.
    """

    contract: CheckContract | None
    failure_reason: str | None = None

    @property
    def passed(self) -> bool:
        """True if the function passed all validation requirements."""
        return self.contract is not None


def _wrap_dataframe_input(
    func: "Callable",
    df_param: str,
    kind: str,
) -> "Callable":
    """Wrap a DataFrame-input function to accept the context dict convention.

    At call time:
      - Extracts df from context, strips pipeline (_) columns
      - Calls func with clean df bound to df_param
      - For kind='check' + DataFrame return: extracts check_col column as bool Series
      - For kind='transform' + DataFrame return: stores overwrite_cols in result attrs

    check_col and overwrite_cols are read from partial keywords — they are baked
    in by _register_check from the params the user configured in vp new report.
    """
    # Extract check_col / overwrite_cols from partial chain keywords
    check_col: str | None = None
    overwrite_cols: list[str] | None = None
    inner = func
    while isinstance(inner, functools.partial):
        if "check_col" in inner.keywords:
            check_col = inner.keywords["check_col"]
        if "overwrite_cols" in inner.keywords:
            overwrite_cols = inner.keywords["overwrite_cols"]
        inner = inner.func

    @functools.wraps(inner)
    def wrapper(context: dict):
        df: "pd.DataFrame" = context["df"]
        user_cols = [c for c in df.columns if not c.startswith("_")]
        clean_df = df[user_cols]

        result = func(**{df_param: clean_df})

        if isinstance(result, pd.DataFrame):
            if kind == "check" and check_col:
                # Extract the boolean check column from the returned DataFrame
                if check_col in result.columns:
                    series = result[check_col]
                    try:
                        return series.astype(bool)
                    except Exception:
                        return pd.Series([False] * len(series), index=series.index)
                # check_col not found — flag all rows
                print(
                    f"[warn] DataFrame check returned a DataFrame but "
                    f"check_col '{check_col}' was not found — all rows flagged."
                )
                return pd.Series([False] * len(result), dtype=bool)

            elif kind == "transform" and overwrite_cols:
                # Tag the result so the transform runner knows which cols to write
                result = result.copy()
                result.attrs["overwrite_cols"] = overwrite_cols

        return result

    return wrapper


def _wrap_series_input(
    func: "Callable",
    series_params: list[str],
) -> "Callable":
    """Wrap a Series-input function to accept the context dict convention.

    For functions with pd.Series params and no pd.DataFrame param.
    The pipeline assembles the call — users write plain functions,
    context: dict never appears in user code.

    At call time:
      - Extracts df from context, strips pipeline (_) columns
      - For each pd.Series param: reads baked column name from partial
        keywords (set by _register_check from alias_map), extracts
        df[col_name] as a Series and injects it by keyword
      - Scalar constants remain in the partial and are passed through
        automatically via the inner function call

    series_params: list of param names annotated as pd.Series,
                   from CheckParamInspector.series_params(). Must be
                   non-empty (caller is responsible for this guard).

    Raises ValueError at call time if a Series param has no baked column
    name (alias_map not configured) or the column is not in the table.
    """
    # Collect all baked keywords from the full partial chain.
    # These contain: Series param → column name string (from alias_map),
    # plus any scalar constants (from filled_params).
    baked_keywords: dict = {}
    inner = func
    while isinstance(inner, functools.partial):
        baked_keywords.update(inner.keywords)
        inner = inner.func

    @functools.wraps(inner)
    def wrapper(context: dict):
        # ── DuckDB-native path ─────────────────────────────────────────────
        # Triggered when _compute_report determines no check needs a full
        # pandas DataFrame. conn remains open (read_only=True) during execution.
        if "conn" in context:
            conn = context["conn"]
            table = context["table"]
            pending_pks = context["pending_pks"]
            pk_col = context["pk_col"]
            col_cache = context.get("col_cache", {})

            call_kwargs = dict(baked_keywords)
            for param_name in series_params:
                col_name = baked_keywords.get(param_name)
                if col_name is None:
                    raise ValueError(
                        f"Series param '{param_name}' has no column name baked in partial. "
                        f"The alias_map for this check is missing an entry for '{param_name}'. "
                        f"Run 'vp edit report' to add the mapping, or check reports_config.yaml."
                    )
                # Read from pre-fetched cache if available — avoids a query per check
                if col_name in col_cache:
                    call_kwargs[param_name] = col_cache[col_name]
                elif pk_col and pending_pks:
                    placeholders = ", ".join("?" * len(pending_pks))
                    col_df = conn.execute(
                        f'SELECT "{col_name}" FROM "{table}"'
                        f' WHERE CAST("{pk_col}" AS VARCHAR) IN ({placeholders})',
                        list(pending_pks),
                    ).df()
                    if col_name not in col_df.columns:
                        raise ValueError(
                            f"Series param '{param_name}' maps to column '{col_name}' "
                            f"which is not in the table."
                        )
                    call_kwargs[param_name] = col_df[col_name]
                else:
                    col_df = conn.execute(f'SELECT "{col_name}" FROM "{table}"').df()
                    if col_name not in col_df.columns:
                        raise ValueError(
                            f"Series param '{param_name}' maps to column '{col_name}' "
                            f"which is not in the table."
                        )
                    call_kwargs[param_name] = col_df[col_name]
            return inner(**call_kwargs)

        # ── Pandas path ────────────────────────────────────────────────────
        df: "pd.DataFrame" = context["df"]
        user_cols = [c for c in df.columns if not c.startswith("_")]
        clean_df = df[user_cols]

        call_kwargs = dict(baked_keywords)
        for param_name in series_params:
            col_name = baked_keywords.get(param_name)
            if col_name is None:
                raise ValueError(
                    f"Series param '{param_name}' has no column name baked in partial. "
                    f"The alias_map for this check is missing an entry for '{param_name}'. "
                    f"Run 'vp edit report' to add the mapping, or check reports_config.yaml."
                )
            if col_name not in clean_df.columns:
                raise ValueError(
                    f"Series param '{param_name}' maps to column '{col_name}' "
                    f"which is not in the table. "
                    f"Available columns: {list(clean_df.columns)}"
                )
            call_kwargs[param_name] = clean_df[col_name]
        return inner(**call_kwargs)

    return wrapper


def _wrap_scalar_column_input(func: "Callable") -> "Callable":
    """Wrap a scalar-input function to accept the context dict convention.

    For functions with str/int/float/unannotated params, no pd.DataFrame,
    no pd.Series, no context: dict. The pipeline assembles the per-row call —
    users write plain scalar functions, context: dict never appears.

    At call time:
      - Extracts df from context, strips pipeline (_) columns
      - For each baked param whose string value matches a column name in df:
        that param is col-backed → apply function per row via pandas .apply(),
        producing a Series result
      - Params whose baked values are not column names are broadcast constants,
        passed through unchanged from the partial
      - Col-backed detection: isinstance(val, str) and val in clean_df.columns
        Same logic as _apply_scalar_transform_duckdb — made at call time so
        the same wrapper works across different reports (different tables).

    Single col-backed param uses Series.apply for efficiency.
    Multiple col-backed params use df.apply(axis=1) for row-wise application.
    """
    baked_keywords: dict = {}
    inner = func
    while isinstance(inner, functools.partial):
        baked_keywords.update(inner.keywords)
        inner = inner.func

    @functools.wraps(inner)
    def wrapper(context: dict):
        # ── DuckDB-native path ─────────────────────────────────────────────
        if "conn" in context:
            conn = context["conn"]
            table = context["table"]
            pending_pks = context["pending_pks"]
            pk_col = context["pk_col"]
            available_cols = context.get("all_columns", [])

            col_backed: dict[str, str] = {}
            constants: dict = {}
            for param_name, val in baked_keywords.items():
                if isinstance(val, str) and val in available_cols:
                    col_backed[param_name] = val
                else:
                    constants[param_name] = val

            if not col_backed:
                return inner(**constants)

            placeholders = ", ".join("?" * len(pending_pks)) if pk_col and pending_pks else None

            if len(col_backed) == 1:
                param_name, col_name = next(iter(col_backed.items()))
                if placeholders and pk_col:
                    col_df = conn.execute(
                        f'SELECT "{col_name}" FROM "{table}"'
                        f' WHERE CAST("{pk_col}" AS VARCHAR) IN ({placeholders})',
                        list(pending_pks),
                    ).df()
                else:
                    col_df = conn.execute(f'SELECT "{col_name}" FROM "{table}"').df()
                result = col_df[col_name].apply(
                    lambda val: inner(**{param_name: val}, **constants)
                )
                result.name = col_name
                return result

            # Multiple col-backed params — load all needed columns, apply row-wise
            needed_cols = list({col for col in col_backed.values()})
            col_list = ", ".join(f'"{c}"' for c in needed_cols)
            if placeholders and pk_col:
                subset_df = conn.execute(
                    f'SELECT {col_list} FROM "{table}"'
                    f' WHERE CAST("{pk_col}" AS VARCHAR) IN ({placeholders})',
                    list(pending_pks),
                ).df()
            else:
                subset_df = conn.execute(f'SELECT {col_list} FROM "{table}"').df()

            def apply_row_duckdb(row):
                row_kwargs = {p: row[col] for p, col in col_backed.items()}
                row_kwargs.update(constants)
                return inner(**row_kwargs)

            return subset_df.apply(apply_row_duckdb, axis=1)

        # ── Pandas path ────────────────────────────────────────────────────
        df: "pd.DataFrame" = context["df"]
        user_cols = [c for c in df.columns if not c.startswith("_")]
        clean_df = df[user_cols]

        col_backed: dict[str, str] = {}
        constants: dict = {}
        for param_name, val in baked_keywords.items():
            if isinstance(val, str) and val in clean_df.columns:
                col_backed[param_name] = val
            else:
                constants[param_name] = val

        if not col_backed:
            return inner(**constants)

        if len(col_backed) == 1:
            param_name, col_name = next(iter(col_backed.items()))
            result = clean_df[col_name].apply(
                lambda val: inner(**{param_name: val}, **constants)
            )
            result.name = col_name
            return result

        needed_cols = list({col for col in col_backed.values()})
        subset = clean_df[needed_cols]

        def apply_row(row):
            row_kwargs = {p: row[col] for p, col in col_backed.items()}
            row_kwargs.update(constants)
            return inner(**row_kwargs)

        return subset.apply(apply_row, axis=1)

    return wrapper


def validate_check(
    name: str,
    func: "Callable",
    kind: str,
) -> "CheckAudit":
    """Validate a function against the check contract and return a CheckAudit.

    Single validation gate for all checks and transforms. Handles both the
    standard pd.Series convention and the new pd.DataFrame-input convention.

    DataFrame-input functions:
      - Any param annotated as pd.DataFrame is auto-filled at call time
      - kind='check' + returns DataFrame: check_col extracted at runtime
      - kind='transform' + returns DataFrame: overwrite_cols applied at runtime

    :param name: Check name for warning messages.
    :param func: Function to validate (may be a partial with params baked in).
    :param kind: 'check' or 'transform'.
    :return: CheckAudit with contract on success, failure_reason on failure.
    """
    from proto_pipe.checks.result import wrap_series_check

    # Resolve original function name once — stored on CheckContract (inspect-once rule).
    _inner = func
    while isinstance(_inner, functools.partial):
        _inner = _inner.func
    import inspect as _inspect
    _inner = _inspect.unwrap(_inner)
    _func_name = getattr(_inner, "__name__", "") or ""

    if kind not in ("check", "transform"):
        reason = (
            f"kind must be 'check' or 'transform', got '{kind}'. "
            f"Change the kind= argument in @custom_check."
        )
        print(f"[warn] `{name}`: {reason}")
        return CheckAudit(contract=None, failure_reason=reason)

    inspector = CheckParamInspector(func)

    # ── DataFrame-input path ─────────────────────────────────────────────────
    if inspector.has_dataframe_input():
        df_param = inspector.dataframe_params()[0]

        if inspector.empty_return_annotation():
            print(
                f"[warn] '{name}': no return annotation — registering as {kind} anyway. "
                f"Consider adding '-> pd.Series', '-> pd.Series[bool]', or '-> pd.DataFrame'."
            )

        elif kind == "check":
            # Check must return Series or DataFrame — not something else
            if (
                not inspector.returns_boolean_series()
                and not inspector.returns_dataframe()
            ):
                reason = (
                    "DataFrame-input check must return pd.Series, pd.Series[bool], "
                    "or pd.DataFrame — skipping registration."
                )
                print(f"[warn] '{name}': {reason}")
                return CheckAudit(contract=None, failure_reason=reason)

        df_wrapped = _wrap_dataframe_input(func, df_param, kind)
        wrapped_func = wrap_series_check(df_wrapped) if kind == "check" else df_wrapped
        return CheckAudit(CheckContract(func=wrapped_func, kind=kind, needs_dataframe=True, func_name=_func_name))

    # ── Series-input path ────────────────────────────────────────────────────
    # Functions with pd.Series params and no pd.DataFrame param.
    # Legacy context: dict functions are routed to the standard path below —
    # has_legacy_context_param() detects them. New annotation-based functions
    # (no context param) get the Series injection wrapper.
    if inspector.has_series_params() and not inspector.has_legacy_context_param():
        series_param_names = inspector.series_params()

        if inspector.empty_return_annotation():
            print(
                f"[warn] '{name}': no return annotation — registering as {kind} anyway. "
                f"Consider adding '-> pd.Series', '-> pd.Series[bool]', or '-> pd.DataFrame'."
            )
        elif kind == "check" and not inspector.returns_boolean_series():
            reason = (
                "Series-input check must return pd.Series[bool] — skipping registration. "
                "Fix the return annotation or use kind='transform'."
            )
            print(f"[warn] '{name}': {reason}")
            return CheckAudit(contract=None, failure_reason=reason)

        series_wrapped = _wrap_series_input(func, series_param_names)
        wrapped_func = wrap_series_check(series_wrapped) if kind == "check" else series_wrapped

        # Extract baked column names from the partial chain — stored once at
        # registration time so _compute_report can pre-fetch all needed columns
        # in a single bulk query without re-inspecting the wrapped function.
        baked: dict = {}
        inner = func
        while isinstance(inner, functools.partial):
            baked.update(inner.keywords)
            inner = inner.func
        col_names = [v for p, v in baked.items() if p in series_param_names and isinstance(v, str)]

        col_backed_map = {p: v for p, v in baked.items() if p in series_param_names and isinstance(v, str)}
        return CheckAudit(CheckContract(
            func=wrapped_func, kind=kind, needs_series=True,
            series_columns=col_names, col_backed_params=col_backed_map,
            func_name=_func_name,
        ))

    # ── Scalar column-backed path ─────────────────────────────────────────────
    # Functions with str/int/float/unannotated params, no pd.DataFrame,
    # no pd.Series, no context: dict. Applied per row at call time — the
    # wrapper detects col-backed params by checking baked string values
    # against clean_df.columns at call time (same as _apply_scalar_transform_duckdb).
    # Scalar checks may return bool (per-row scalar) or pd.Series[bool] (vectorised).
    #
    # Gate: both legacy name ('context') and positional context patterns
    # (unannotated first param, dict-annotated first param) are routed to
    # the standard path to preserve backward compatibility.
    if not inspector.has_legacy_context_param() and not inspector.has_positional_context_param():
        if inspector.empty_return_annotation():
            if kind == "check":
                reason = (
                    "no return annotation found — skipping registration. "
                    "Add '-> bool' or '-> pd.Series[bool]' to your check function."
                )
                print(f"[warn] '{name}': {reason}")
                return CheckAudit(contract=None, failure_reason=reason)
            else:
                print(
                    f"[warn] '{name}': no return annotation — registering as {kind} anyway. "
                    f"Consider adding '-> bool' or '-> pd.Series[bool]' for checks, "
                    f"or '-> pd.Series' for transforms."
                )
        elif kind == "check":
            if (
                not inspector.returns_boolean_series()
                and not inspector.returns_scalar_bool()
            ):
                reason = (
                    "Scalar check must return bool or pd.Series[bool] — "
                    "skipping registration. Fix the return annotation or use kind='transform'."
                )
                print(f"[warn] '{name}': {reason}")
                return CheckAudit(contract=None, failure_reason=reason)
        elif kind == "transform" and inspector.returns_boolean_series():
            print(
                f"[warn] '{name}' is kind='transform' but its return annotation is "
                f"pd.Series[bool] — this looks like a check. "
                f"Registering as transform anyway. Did you mean kind='check'?"
            )

        scalar_wrapped = _wrap_scalar_column_input(func)
        wrapped_func = wrap_series_check(scalar_wrapped) if kind == "check" else scalar_wrapped
        return CheckAudit(CheckContract(func=wrapped_func, kind=kind, is_scalar=True, func_name=_func_name))
    if inspector.empty_return_annotation():
        if kind == "check":
            reason = (
                "no return annotation found — skipping registration. "
                "Add '-> pd.Series' to your check function signature."
            )
            print(f"[warn] '{name}': {reason}")
            return CheckAudit(contract=None, failure_reason=reason)
        else:
            print(
                f"[warn] '{name}': no return annotation found — registering as transform anyway. "
                f"Consider adding '-> pd.Series' or '-> pd.DataFrame' for clarity."
            )

    returns_bool = inspector.returns_boolean_series()
    if kind == "check" and not returns_bool:
        reason = (
            "return annotation is not pd.Series[bool] — skipping registration. "
            "Fix the annotation or use kind='transform'."
        )
        print(f"[warn] '{name}': {reason}")
        return CheckAudit(contract=None, failure_reason=reason)

    if kind == "transform" and returns_bool:
        print(
            f"[warn] '{name}' is kind='transform' but its return annotation is "
            f"pd.Series[bool] — this looks like a check. "
            f"Registering as transform anyway. Did you mean kind='check'?"
        )

    wrapped_func = wrap_series_check(func) if kind == "check" else func
    return CheckAudit(CheckContract(func=wrapped_func, kind=kind, is_legacy=True, func_name=_func_name))


class CheckParamInspector:
    """Inspects a check function to determine its param types and multiselect eligibility.

    Usage:
        inspector = CheckParamInspector(my_check_func)
        if inspector.is_multiselect_eligible():
            cols = inspector.column_params()
    """

    def __init__(self, func: Callable):
        import functools

        # TODO: Check if all these unwraps are necessary
        # Pass 1: unwrap partial chain
        unwrapped = func
        while isinstance(unwrapped, functools.partial):
            unwrapped = unwrapped.func

        # Pass 2: follow __wrapped__ (wrap_series_check sets this)
        unwrapped = inspect.unwrap(unwrapped)

        # Pass 3: unwrap any partial __wrapped__ pointed to
        while isinstance(unwrapped, functools.partial):
            unwrapped = unwrapped.func

        self.func = unwrapped
        self._sig = inspect.signature(self.func)
        self._source = inspect.getsource(self.func)

    def returns_boolean_series(self) -> bool:
        """True if the function's return annotation is pd.Series or Series."""
        ann = self._sig.return_annotation
        if ann is inspect.Parameter.empty:
            return False
        # Accept pd.Series, 'pd.Series', 'pd.Series[bool]'
        ann_str = str(ann) if not isinstance(ann, str) else ann
        return "Series" in ann_str

    def returns_scalar_bool(self) -> bool:
        """True if the return annotation is a scalar bool (not pd.Series[bool]).

        Valid for scalar-input checks (str/int/float params) where the wrapper
        assembles the per-row Series from scalar bool returns via apply().
        Callers must use this method — never access _sig.return_annotation directly.
        """
        ann = self._sig.return_annotation
        if ann is inspect.Parameter.empty:
            return False
        return ann is bool or str(ann) in ("bool", "<class 'bool'>")

    def empty_return_annotation(self) -> bool:
        """True if the function has no return annotation."""
        return self._sig.return_annotation is inspect.Parameter.empty

    def is_multiselect_eligible(self) -> bool:
        """True if the function returns pd.Series AND has at least one expandable param.

        Expandable params include column selectors (str/Series/unannotated) and
        column-backed scalars (int/float when no pd.Series params present). Both
        go through the same alias_map expansion process.
        """
        return self.returns_boolean_series() and (
            len(self.column_params()) > 0
            or len(self.column_backed_scalar_params()) > 0
        )

    def has_series_params(self) -> bool:
        """True if the function has any pd.Series-annotated params.

        When True, the highest-granularity gate applies: all non-Series,
        non-DataFrame params skip the column picker and go straight to
        constant entry. No type exceptions.
        """
        return any(
            is_series_annotation(param.annotation)
            for name, param in self._sig.parameters.items()
            if name != "context"
        )

    def has_legacy_context_param(self) -> bool:
        """True if the function has a 'context' parameter (legacy calling convention).

        Legacy functions take context: dict as their first param and access
        df via context["df"] themselves. They must use the standard execution
        path, not the Series injection wrapper.

        Used by validate_check to route legacy functions away from _wrap_series_input.
        Callers must use this method — never access _sig.parameters directly.
        """
        return "context" in self._sig.parameters

    def has_positional_context_param(self) -> bool:
        """True if the function's first positional param looks like a context dict.

        Catches informal legacy patterns beyond the documented 'context: dict'
        convention — e.g. 'ctx', 'data', 'ctx: dict'. Any unannotated first
        positional param with no default, or a dict-annotated first param, is
        treated as a positional context arg and routed to the standard path.

        Callers must use this method — never access _sig.parameters directly.
        """
        params = list(self._sig.parameters.values())
        if not params:
            return False
        first = params[0]
        if first.kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            return False
        if first.default is not inspect.Parameter.empty:
            return False
        if first.annotation is inspect.Parameter.empty:
            # Unannotated first param with no default — positional context arg
            return True
        # dict-annotated first param (ctx: dict) — legacy context dict
        ann = first.annotation
        return ann is dict or str(ann) in ("dict", "<class 'dict'>")

    def series_params(self) -> list[str]:
        """Return param names annotated as pd.Series.

        Subset of column_params(). Used by prompts.py to distinguish Series params
        (column picker, no escape hatch) from str/unannotated params (column picker
        WITH escape hatch). Callers must use this method — never access _sig directly.
        """
        return [
            name for name, param in self._sig.parameters.items()
            if name != "context"
            and is_series_annotation(param.annotation)
        ]

    def column_backed_scalar_params(self) -> list[str]:
        """Return int/float param names eligible for column-backing.

        Returns scalar_params() when has_series_params() is False — these params
        can be column-backed (alias_map entry, per-row DuckDB UDF values) or
        broadcast constants (filled_params), decided by the user at prompt time.

        Returns [] when any pd.Series param exists (highest-granularity gate):
        Series params already provide per-row data, so int/float params must
        remain broadcast constants. No type exceptions to this rule.
        """
        if self.has_series_params():
            return []
        return self.scalar_params()

    def is_expandable(self) -> bool:
        """True if this function can be meaningfully expanded N times per alias_map entry.

        Checks returning pd.Series[bool] produce separate validation results per expansion.
        Transforms returning pd.Series or pd.DataFrame produce separate column updates.
        Functions returning dict, int, or other non-data types must not be expanded —
        the semantics of per-column expansion are undefined for non-Series/DataFrame returns.

        Used by _expand_check_with_alias_map in io/registry.py as the expansion gate,
        replacing the narrower is_multiselect_eligible() check which only covers bool-Series
        checks and excluded transforms.
        """
        import inspect as _inspect
        ann = self._sig.return_annotation
        if ann is _inspect.Parameter.empty:
            return False
        ann_str = str(ann)
        return "Series" in ann_str or "DataFrame" in ann_str

    def all_expandable_param_names(self) -> list[str]:
        """Return all param names eligible for alias_map expansion.

        Covers column selectors (str/Series/unannotated) and column-backed
        scalars (int/float). Excludes DataFrame params and the _output reserved key.
        Used by _expand_check_with_alias_map in io/registry.py — callers must use
        this method rather than accessing _sig directly (CLAUDE.md: CheckParamInspector
        is the canonical inspection pattern).
        """
        return [
            name
            for name, param in self._sig.parameters.items()
            if name != "context"
            and not is_dataframe_annotation(param.annotation)
        ]

    def make_key(self) -> str:
        """Return a deterministic hash of the function source.

        Changes when the function body changes — used to detect stale metadata.
        """
        return hashlib.md5(self._source.encode()).hexdigest()

    def column_params(self) -> list[str]:
        """Return param names that are column selectors.

        A param is a column selector when its annotation is:
          - str / "str" → column name passed as string
          - pd.Series / "pd.Series" → column data passed as a Series
          - unannotated → treated as a column selector by convention

        DataFrame params are excluded — they are auto-filled with the full table.
        String annotations produced by `from __future__ import annotations` are
        handled correctly by is_str_annotation and is_series_annotation.
        """
        return [
            name
            for name, param in self._sig.parameters.items()
            if name != "context"
            and (
                is_str_annotation(param.annotation)
                or param.annotation is inspect.Parameter.empty
                or is_series_annotation(param.annotation)
            )
            and not is_dataframe_annotation(param.annotation)
        ]

    def scalar_params(self) -> list[str]:
        """Return param names that are scalar values (non-column, non-DataFrame).

        Scalars are params with explicit non-str, non-Series, non-DataFrame
        annotations — e.g. min_val: float, threshold: int. These are broadcast
        across all runs of the check (alias_map does not apply to them).

        Unannotated params are treated as column selectors, not scalars.
        """
        return [
            name
            for name, param in self._sig.parameters.items()
            if name != "context"
            and param.annotation is not inspect.Parameter.empty
            and not is_str_annotation(param.annotation)
            and not is_series_annotation(param.annotation)
            and not is_dataframe_annotation(param.annotation)
        ]

    def dataframe_params(self) -> list[str]:
        """Return param names annotated as pd.DataFrame.

        These are auto-filled at call time with the report's cleaned table
        (pipeline columns stripped). They are never prompted in vp new report.
        """
        return [
            name
            for name, param in self._sig.parameters.items()
            if name != "context" and is_dataframe_annotation(param.annotation)
        ]

    def has_dataframe_input(self) -> bool:
        """True if the function accepts a pd.DataFrame parameter."""
        return len(self.dataframe_params()) > 0

    def returns_dataframe(self) -> bool:
        """True if the return annotation is pd.DataFrame."""
        ann = self._sig.return_annotation
        if ann is inspect.Parameter.empty:
            return False
        ann_str = str(ann) if not isinstance(ann, str) else ann
        return "DataFrame" in ann_str

    def write_to_db(
        self,
        conn: duckdb.DuckDBPyConnection,
        check_name: str,
    ) -> None:
        """Store or update check metadata in check_registry_metadata.

        Computes inspection values and delegates all SQL to
        upsert_check_metadata in io/db.py — checks/ never executes
        raw SQL against pipeline tables (module responsibility rule).
        """
        from proto_pipe.io.db import upsert_check_metadata

        upsert_check_metadata(
            conn,
            check_name=check_name,
            check_key=self.make_key(),
            func_name=getattr(self.func, "__name__", ""),
            is_multiselect_eligible=self.is_multiselect_eligible(),
            column_params=", ".join(self.column_params()),
            scalar_params=", ".join(self.scalar_params()),
        )
