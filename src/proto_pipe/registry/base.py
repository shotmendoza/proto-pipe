from functools import partial
from typing import Callable

from proto_pipe.checks.result import CheckResult, wrap_series_check


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


class CheckRegistry:
    """One of the main parts of `proto-pipe`. This object is used as a source of truth
    for all templates and checks to *registry* into, so that the checks are applied and run.

    """
    def __init__(self):
        """The checks / functions that have been registered. Key-Value pair of function name, function"""
        self._checks: dict[str, Callable] = {}

    def get(self, name: str) -> Callable | None:
        """Return the registered function for a check name, or None if not found."""
        return self._checks.get(name)

    def register(self, name: str, func: Callable) -> None:
        """Register a function into the registry.

        If the function returns pd.Series[bool], it is automatically wrapped
        to return CheckResult. If it has no return annotation or an invalid one,
        registration is rejected with a clear error.

        :param name: name of the function, used as a reference
        :param func: function to be registered.
        :raises ValueError: if the function lacks a valid return annotation
        """
        from proto_pipe.checks.inspector import CheckParamInspector

        # Get the underlying function if wrapped by partial
        inspector = CheckParamInspector(func)

        if inspector.returns_boolean_series():
            self._checks[name] = wrap_series_check(func)
        else:
            raise ValueError(
                f"[error] Check '{name}' was not registered.\n"
                f"Reason: missing return annotation.\n"
                f"Fix: add '-> pd.Series' to your function signature.\n"
                f"Example: def {inspector.func.__name__}(context, col: str) -> pd.Series: ..."
            )

    def register_with_params(
            self,
            name: str,
            func: Callable,
            **params
    ) -> None:
        """Bake params into the function at registration time using partial.

        This will allow parameters to be saved, and executed when the function is called and ready.

        :param name: name of the function, used as a reference
        :param func: function to be registered.
        """
        self.register(name, partial(func, **params))

    def run(
            self,
            name: str,
            context: dict
    ):
        """function for running specific checks in the registry

        :param name: name of the function, used as a reference
        :param context: the parameters to pass to the function
        """
        if name not in self._checks:
            raise ValueError(f"No check registered under '{name}'")
        return self._checks[name](context)

    def available(self) -> list[str]:
        """returns a list of available checks, that were registered

        :return: available checks
        """
        return list(self._checks.keys())


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
