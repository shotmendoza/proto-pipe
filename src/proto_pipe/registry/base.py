from functools import partial
from typing import Callable

from proto_pipe.checks.inspector import CheckContract, validate_check


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
