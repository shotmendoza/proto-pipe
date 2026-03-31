from functools import partial
from typing import Callable


# ---------------------------------------------------------------
# Check Registry
#
# The idea behind the registry is that you "register" functions
# that you want to run all together.
#
# The strength of the registry is that you can use an arbitrary
# parameter set, and they would all run in the same way. This
# makes it so whatever object is running the functions, do not
# need to be aware of the shape of the functions, thus, decoupling
# the tie between an object needing to know the shape, and the
# object running it.
#
# Each entry stores (func, kind) where kind is "check" or
# "transform". The runner uses get_kind() to decide how to handle
# the result — checks go to validation_flags, transforms are
# applied back to the report table.
# ----------------------------------------------------------------


class CheckRegistry:
    """One of the main parts of `proto-pipe`. This object is used as a source of truth
    for all templates and checks to *register* into, so that the checks are applied and run.
    """

    def __init__(self):
        self._checks: dict[str, tuple[Callable, str]] = {}
        """Maps name -> (function, kind). kind is 'check' or 'transform'."""

    def register(self, name: str, func: Callable, kind: str = "check") -> None:
        """Register a function into the registry.

        :param name: name of the function, used as a reference
        :param func: function to be registered.
        :param kind: "check" (default) or "transform".
        """
        if kind not in ("check", "transform"):
            raise ValueError(f"kind must be 'check' or 'transform', got '{kind}'")
        self._checks[name] = (func, kind)

    def register_with_params(
            self,
            name: str,
            func: Callable,
            kind: str = "check",
            **params
    ) -> None:
        """Bake params into the function at registration time using partial.

        This will allow parameters to be saved, and executed when the function is
        called and ready.

        :param name: name of the function, used as a reference
        :param func: function to be registered.
        :param kind: "check" (default) or "transform".
        """
        self._checks[name] = (partial(func, **params), kind)

    def run(self, name: str, context: dict):
        """Run a specific check or transform in the registry.

        :param name: name of the function, used as a reference
        :param context: the parameters to pass to the function (must contain "df")
        """
        if name not in self._checks:
            raise ValueError(f"No check registered under '{name}'")
        func, _ = self._checks[name]
        return func(context)

    def get(self, name: str) -> Callable | None:
        """Return the registered callable for name, or None if not found.

        :param name: name of the function
        :return: the callable, or None
        """
        if name not in self._checks:
            return None
        func, _ = self._checks[name]
        return func

    def get_kind(self, name: str) -> str:
        """Return the kind ("check" or "transform") for a registered name.

        :param name: name of the function
        :return: "check" or "transform"
        :raises ValueError: if name is not registered
        """
        if name not in self._checks:
            raise ValueError(f"No check registered under '{name}'")
        _, kind = self._checks[name]
        return kind

    def available(self) -> list[str]:
        """Return a list of all registered names (checks and transforms).

        :return: available check/transform names
        """
        return list(self._checks.keys())

    def checks_only(self) -> list[str]:
        """Return names of registered entries with kind='check'."""
        return [n for n, (_, k) in self._checks.items() if k == "check"]

    def transforms_only(self) -> list[str]:
        """Return names of registered entries with kind='transform'."""
        return [n for n, (_, k) in self._checks.items() if k == "transform"]


class ReportRegistry:
    """Stores report definitions (name, source config, check names, options).
    Knows nothing about check implementations — references checks by name only.
    The runner mediates between ReportRegistry and CheckRegistry.
    """

    def __init__(self):
        self._reports: dict[str, dict] = {}
        """Maps report name -> report config dict."""

    def register(self, name: str, report_config: dict) -> None:
        """Register a new report configuration.

        The method associates a report name with its corresponding configuration.
        If the report name already exists, the old configuration is overwritten.

        :param name: The unique identifier for the report.
        :param report_config: A dictionary containing the configuration details.
        :return: None
        """
        self._reports[name] = report_config

    def get(self, name: str) -> dict:
        """Fetch a report from the list of available reports.

        :param name: The name of the report to fetch.
        :return: A dictionary containing the details of the requested report.
        :raises ValueError: If no report is registered under the provided name.
        """
        if name not in self._reports:
            raise ValueError(f"No report registered under '{name}'")
        return self._reports[name]

    def all(self) -> list[dict]:
        """Return all registered report configs.

        :return: A list of report config dicts.
        """
        return list(self._reports.values())

    def available(self) -> list[str]:
        """Return a list of available report names.

        :return: List of report name strings.
        """
        return list(self._reports.keys())


# Global registry instance
check_registry = CheckRegistry()
report_registry = ReportRegistry()
