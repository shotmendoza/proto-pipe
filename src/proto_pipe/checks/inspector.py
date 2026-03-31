"""CheckParamInspector — inspects a check function's signature and stores metadata."""
from __future__ import annotations

import hashlib
import inspect
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

import duckdb


@dataclass
class CheckContract:
    """A vetted check or transform function, ready for the CheckRegistry.

    Created exclusively by validate_check() after passing all validation.
    CheckRegistry stores and communicates through CheckContract objects —
    anything in the registry has been signed off by CheckParamInspector.

    Attributes:
        func: The callable. For kind='check', already wrapped with
              wrap_series_check so it returns CheckResult.
        kind: 'check' or 'transform'.
    """

    func: Callable
    kind: str = "check"


def validate_check(
    name: str,
    func: Callable,
    kind: str,
) -> "CheckContract | None":
    """Validate a function against the check contract and return a CheckContract.

    This is the single validation gate for all checks and transforms. Uses
    CheckParamInspector to inspect the function. Returns None (and prints a
    user-facing warning) if any requirement is not met — invalid functions
    are never registered.

    Requirements:
      - kind must be 'check' or 'transform'
      - function must have a return annotation
      - kind='check' must return pd.Series[bool]
      - kind='transform' returning pd.Series[bool] is allowed but warned

    For kind='check', the returned contract's func is already wrapped with
    wrap_series_check so it produces a CheckResult at runtime.

    :param name: The check name, used in warning messages.
    :param func: The function to validate.
    :param kind: The intended kind — 'check' or 'transform'.
    :return: A CheckContract if valid, None if validation failed.
    """
    from proto_pipe.checks.result import wrap_series_check

    if kind not in ("check", "transform"):
        print(
            f"[warn] '{name}': kind must be 'check' or 'transform', "
            f"got '{kind}' — skipping registration."
        )
        return None

    inspector = CheckParamInspector(func)

    if inspector._sig.return_annotation is inspect.Parameter.empty:
        if kind == "check":
            print(
                f"[warn] '{name}': no return annotation found — skipping registration. "
                f"Add '-> pd.Series' to your check function signature."
            )
            return None
        else:
            print(
                f"[warn] '{name}': no return annotation found — registering as transform anyway. "
                f"Consider adding '-> pd.Series' or '-> pd.DataFrame' for clarity."
            )

    returns_bool = inspector.returns_boolean_series()

    if kind == "check" and not returns_bool:
        print(
            f"[warn] '{name}' is kind='check' but its return annotation is not "
            f"pd.Series[bool] — skipping registration. "
            f"Fix the annotation or use kind='transform'."
        )
        return None

    if kind == "transform" and returns_bool:
        print(
            f"[warn] '{name}' is kind='transform' but its return annotation is "
            f"pd.Series[bool] — this looks like a check. "
            f"Registering as transform anyway. Did you mean kind='check'?"
        )

    wrapped_func = wrap_series_check(func) if kind == "check" else func
    return CheckContract(func=wrapped_func, kind=kind)


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

    def column_params(self) -> list[str]:
        """Return param names that are str type — these are column selectors."""
        return [
            name for name, param in self._sig.parameters.items()
            if name != "context"
            and param.annotation in (str, inspect.Parameter.empty)
        ]

    def scalar_params(self) -> list[str]:
        """Return param names that are non-str types — these are scalar values."""
        return [
            name for name, param in self._sig.parameters.items()
            if name != "context"
            and param.annotation not in (str, inspect.Parameter.empty)
        ]

    def is_multiselect_eligible(self) -> bool:
        """True if the function returns pd.Series AND has at least one column param."""
        return self.returns_boolean_series() and len(self.column_params()) > 0

    def make_key(self) -> str:
        """Return a deterministic hash of the function source.

        Changes when the function body changes — used to detect stale metadata.
        """
        return hashlib.md5(self._source.encode()).hexdigest()

    def write_to_db(
        self,
        conn: duckdb.DuckDBPyConnection,
        check_name: str,
    ) -> None:
        """Store or update check metadata in check_registry_metadata.

        Safe to call multiple times — updates the record if the key changed
        (function was modified), no-op if key is unchanged.
        """
        key = self.make_key()
        existing = conn.execute(
            "SELECT check_key FROM check_registry_metadata WHERE check_name = ?",
            [check_name],
        ).fetchone()

        now = datetime.now(timezone.utc)

        if existing is None:
            conn.execute("""
                INSERT INTO check_registry_metadata
                    (id, check_name, check_key, is_multiselect_eligible,
                     column_params, scalar_params, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                str(uuid.uuid4()),
                check_name,
                key,
                self.is_multiselect_eligible(),
                ", ".join(self.column_params()),
                ", ".join(self.scalar_params()),
                now,
            ])
        elif existing[0] != key:
            # Function changed — update metadata
            conn.execute("""
                UPDATE check_registry_metadata
                SET check_key = ?,
                    is_multiselect_eligible = ?,
                    column_params = ?,
                    scalar_params = ?,
                    recorded_at = ?
                WHERE check_name = ?
            """, [
                key,
                self.is_multiselect_eligible(),
                ", ".join(self.column_params()),
                ", ".join(self.scalar_params()),
                now,
                check_name,
            ])
        # If key matches, no update needed
