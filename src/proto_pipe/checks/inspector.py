"""CheckParamInspector — inspects a check function's signature and stores metadata."""
from __future__ import annotations

import functools
import hashlib
import inspect
import uuid
from datetime import datetime, timezone
from typing import Callable

import duckdb


class CheckParamInspector:
    """Inspects a check function to determine its param types and multiselect eligibility.

    Usage:
        inspector = CheckParamInspector(my_check_func)
        if inspector.is_multiselect_eligible():
            cols = inspector.column_params()
    """

    def __init__(self, func: Callable):
        # Unwrap partial first, then follow __wrapped__
        unwrapped = func
        while isinstance(unwrapped, functools.partial):
            unwrapped = unwrapped.func
        self.func = inspect.unwrap(unwrapped)
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
