"""Settings module.

Reads and writes pipeline.yaml — the project-level defaults file.
Every CLI command loads settings first so the user never has to
re-enter paths. Individual flags can still override at runtime.
"""

from pathlib import Path

from proto_pipe.io.registry import load_config, write_config

# Default location — can be overridden via PIPELINE_SETTINGS env var
DEFAULT_SETTINGS_PATH = Path("pipeline.yaml")

_DEFAULTS = {
    "paths": {
        "sources_config": "config/sources_config.yaml",
        "reports_config": "config/reports_config.yaml",
        "deliverables_config": "config/deliverables_config.yaml",
        "views_config": "config/views_config.yaml",
        "pipeline_db": "data/pipeline.db",
        "watermark_db": "data/watermarks.db",
        "incoming_dir": "data/incoming/",
        "output_dir": "output/reports/",
        "sql_dir": "sql/",
    },
    "multi_select_params": True,

}


def load_settings(path: Path = DEFAULT_SETTINGS_PATH) -> dict:
    """Load application settings from a YAML file and merge them with default settings.

    This function reads a settings file specified by its path and updates the default
    settings with the loaded values. If the file does not exist, it returns the default
    settings without modification. Additionally, the function ensures that the "paths"
    section of the settings is updated with the corresponding values from the loaded file.

    :param path: Path to the YAML file containing the settings configuration.
    :type path: Path
    :return: A dictionary containing the merged settings.
    :rtype: dict
    """
    if not path.exists():
        return _DEFAULTS.copy()
    loaded = load_config(path)
    # Merge with defaults so missing keys don't cause KeyErrors
    merged = _DEFAULTS.copy()
    merged["paths"].update(loaded.get("paths", {}))
    return merged


def save_settings(settings: dict, path: Path = DEFAULT_SETTINGS_PATH) -> None:
    """Saves the given settings dictionary to a specified file path in YAML format. If no path is
    provided, it uses the default settings path. The function ensures that the settings are
    written with explicit formatting and without sorting the keys.

    :param settings: A dictionary containing configuration settings to be saved.
    :param path: The file path where the settings should be saved. Defaults to the global constant DEFAULT_SETTINGS_PATH
    :return: None
    """
    write_config(config=settings, config_path=path)


def get_path(key: str, path: Path = DEFAULT_SETTINGS_PATH) -> str:
    """Retrieves a specific path associated with a provided key from the settings file.

    The function loads settings from the specified path or defaults to a predefined
    settings path. It retrieves the path corresponding to the given key.

    :param key: The key whose associated path is to be retrieved.
    :type key: str
    :param path: The path to the settings file. If not provided, it defaults to the
        predefined `DEFAULT_SETTINGS_PATH`.
    :type path: Path
    :return: The path associated with the provided key as a string.
    :rtype: str
    """
    return load_settings(path)["paths"][key]


def set_path(key: str, value: str, path: Path = DEFAULT_SETTINGS_PATH) -> None:
    """Updates the path configuration for a given key in the settings file.

    This function modifies the specified path configuration in the settings
    stored at the given file path. If the specified key is not present in the
    paths configuration, a ValueError will be raised. Upon successful modification,
    the updated settings are saved back.

    :param key: The key identifying the path to update in the settings.
    :param value: The new value to assign to the specified path key.
    :param path: The file path of the settings file to modify. Defaults to
        `DEFAULT_SETTINGS_PATH`.
    :return: Nothing is returned.
    :raises ValueError: If the `key` is not a valid path key in the settings.
    """
    settings = load_settings(path)
    if key not in settings["paths"]:
        raise ValueError(
            f"Unknown path key '{key}'. "
            f"Valid keys: {list(settings['paths'].keys())}"
        )
    settings["paths"][key] = value
    save_settings(settings, path)


VALID_PATH_KEYS = list(_DEFAULTS["paths"].keys())
