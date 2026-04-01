from proto_pipe.io.config import config_settings
from proto_pipe.registry.base import CheckRegistry


def load_custom_checks(check_registry: CheckRegistry) -> None:
    """Loads custom checks into the given check registry by importing a custom checks
    module defined in the configuration settings. If no custom checks module is
    specified in the configuration settings, this function performs no action.

    :param check_registry: The registry where custom checks will be loaded.
    :return: None
    """
    from proto_pipe.io.registry import load_custom_checks_module

    module_path = config_settings().get("custom_checks_module")
    if module_path:
        load_custom_checks_module(module_path, check_registry)
