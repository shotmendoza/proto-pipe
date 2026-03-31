"""CheckResult — the contract between check functions and the flagging system."""
from dataclasses import dataclass, field
from typing import Callable

import pandas as pd


@dataclass
class CheckResult:
    """Returned by every check function.

    passed — True if no rows failed
    mask — boolean pd.Series, True where rows FAIL (same index as input df)
    reason — human readable summary of the failure
    metadata — any extra info the check wants to pass along (col, min_val, etc.)
    available — False if the result could not be produced (e.g. bad return type)
    error — set when available=False, relayed to the user
    """
    passed: bool
    """True if no rows failed"""
    mask: pd.Series | None
    """boolean pd.Series, True where rows FAIL (same index as input df)"""
    reason: str
    """reason — human readable summary of the failure"""
    metadata: dict = field(default_factory=dict)
    """any extra info the check wants to pass along (col, min_val, etc.)"""
    available: bool = True
    """available — False if the result could not be produced (e.g. bad return type)"""
    error: str | None = None
    """set when available=False, relayed to the user"""

    def __post_init__(self):
        if self.mask is not None:
            if not isinstance(self.mask, pd.Series):
                self.available = False
                self.error = (
                    f"mask must be pd.Series, got {type(self.mask).__name__}"
                )
            elif self.mask.dtype != bool:
                self.available = False
                self.error = (
                    f"mask must be bool dtype, got {self.mask.dtype}"
                )

    @classmethod
    def from_series(
        cls,
        series: pd.Series,
        reason_prefix: str = "",
        metadata: dict | None = None,
    ) -> "CheckResult":
        """Build a CheckResult from a boolean pd.Series.

        True in the series means the row PASSES.
        False means the row FAILS.
        """
        if not isinstance(series, pd.Series):
            return cls(
                passed=False,
                mask=None,
                reason="",
                available=False,
                error=f"Expected pd.Series, got {type(series).__name__}",
            )
        if series.dtype != bool:
            # Attempt coercion
            try:
                series = series.astype(bool)
            except Exception:
                return cls(
                    passed=False,
                    mask=None,
                    reason="",
                    available=False,
                    error=f"Could not coerce Series to bool, dtype={series.dtype}",
                )

        mask = ~series  # True where rows FAIL
        failed = int(mask.sum())
        passed = failed == 0
        reason = (
            ""
            if passed
            else f"{reason_prefix}{failed} row(s) failed" if not reason_prefix
            else f"{reason_prefix}: {failed} row(s) failed"
        )
        return cls(
            passed=passed,
            mask=mask,
            reason=reason,
            metadata=metadata or {},
        )

    @classmethod
    def unavailable(cls, error: str) -> "CheckResult":
        """Convenience constructor for unavailable checks."""
        return cls(
            passed=False,
            mask=None,
            reason="",
            available=False,
            error=error,
        )


def wrap_series_check(func: Callable) -> Callable:
    """A decorator function to wrap a callable and ensure its return value is processed
    into a `CheckResult` object using the `from_series` method. This wrapper also
    allows passing metadata in the form of keyword arguments.

    :param func: The callable function to be wrapped.
    :type func: Callable
    :return: A callable that wraps the original function, ensuring its return value
        is converted to a `CheckResult` object.
    :rtype: Callable
    """
    def wrapper(context, **kwargs):
        result = func(context, **kwargs)
        return CheckResult.from_series(result, metadata={"kwargs": kwargs})
    wrapper.__wrapped__ = func  # ← add this
    return wrapper

