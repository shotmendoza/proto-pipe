from src.io.settings import load_settings
from src.registry.base import CheckRegistry


def config_settings():
    """A utility function to load and return configurations.

    This function serves as a wrapper for loading application
    settings or configurations using the load_settings method.
    It operates internally and is not intended to be accessed
    outside the module.

    :return: Loaded application settings or configurations.
    """
    return load_settings()


def config_path_or_override(
        key: str,
        override: str | None = None
) -> str:
    """Return a path from settings, optionally overridden by a CLI flag.

    :param key: The key used to identify the path in the settings dictionary.
    :param override: An optional value that overrides the value fetched from the settings dictionary. Defaults to None.
    :return: The path configuration value, either from the settings dictionary or the override if provided.
    """
    return override or config_settings()["paths"][key]


def load_custom_checks(check_registry: CheckRegistry) -> None:
    """Loads custom checks into the given check registry by importing a custom checks
    module defined in the configuration settings. If no custom checks module is
    specified in the configuration settings, this function performs no action.

    :param check_registry: The registry where custom checks will be loaded.
    :return: None
    """
    from src.io.registry import load_custom_checks_module

    module_path = config_settings().get("custom_checks_module")
    if module_path:
        load_custom_checks_module(module_path, check_registry)
