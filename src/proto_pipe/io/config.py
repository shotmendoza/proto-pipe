"""SourceConfig — owns all read/write/query operations against sources_config.yaml.

All code that previously did raw dict access on the sources config
(load_config / iterate sources / source.get("primary_key")) should use
this class instead.
"""
from __future__ import annotations

from os import PathLike
from pathlib import Path

from ruamel.yaml import YAML

from proto_pipe.constants import _DEFAULTS, DEFAULT_SETTINGS_PATH


# TODO: Create an abstract class for the Config. Lot's of shared methods.


class SourceConfig:
    """Wraps sources_config.yaml with typed accessors and write methods.

    Loads the config on construction and keeps it in memory. Writes are
    flushed to disk immediately via write_config.

    Usage:
        config = SourceConfig(src_cfg_path)
        source = config.get_by_table("sales")
        config.add({"name": "sales", "target_table": "sales", ...})
    """

    def __init__(self, path: str | PathLike) -> None:
        self._path = Path(path)
        self._data: dict = load_config(self._path) if self._path.exists() else {}

    # ---------------------------------------------------------------------------
    # Read
    # ---------------------------------------------------------------------------

    def all(self) -> list[dict]:
        """Return all source definitions."""
        return list(self._data.get("sources", []))

    def names(self) -> list[str]:
        """Return all source names."""
        return [s["name"] for s in self.all()]

    def get(self, name: str) -> dict | None:
        """Return the source definition for a given name, or None."""
        return next((s for s in self.all() if s["name"] == name), None)

    def get_by_table(self, table: str) -> dict | None:
        """Return the source definition whose target_table matches, or None."""
        return next((s for s in self.all() if s.get("target_table") == table), None)

    def primary_key(self, table: str) -> str | None:
        """Return the primary key for a table, or None if not defined."""
        source = self.get_by_table(table)
        return source.get("primary_key") if source else None

    def patterns_for(self, name: str) -> list[str]:
        """Return file patterns for a source name."""
        source = self.get(name)
        return source.get("patterns", []) if source else []

    def as_list(self) -> list[dict]:
        """Return all sources as a list — alias for all() for clarity at call sites."""
        return self.all()

    # ---------------------------------------------------------------------------
    # Write
    # ---------------------------------------------------------------------------

    def add(self, source: dict) -> None:
        """Add a new source entry and write to disk.

        :raises ValueError: If a source with the same name already exists.
        """
        if source["name"] in self.names():
            raise ValueError(
                f"Source '{source['name']}' already exists. Use update() to modify it."
            )
        sources = self.all()
        sources.append(source)
        self._data["sources"] = sources
        self._flush()

    def update(self, name: str, source: dict) -> None:
        """Replace an existing source entry and write to disk.

        :raises ValueError: If no source with that name exists.
        """
        sources = self.all()
        idx = next((i for i, s in enumerate(sources) if s["name"] == name), None)
        if idx is None:
            raise ValueError(f"No source named '{name}' found.")
        sources[idx] = source
        self._data["sources"] = sources
        self._flush()

    def add_or_update(self, source: dict) -> None:
        """Add the source if it doesn't exist, otherwise update it."""
        if source["name"] in self.names():
            self.update(source["name"], source)
        else:
            self.add(source)

    def remove(self, name: str) -> None:
        """Remove a source entry and write to disk.

        :raises ValueError: If no source with that name exists.
        """
        sources = self.all()
        remaining = [s for s in sources if s["name"] != name]
        if len(remaining) == len(sources):
            raise ValueError(f"No source named '{name}' found.")
        self._data["sources"] = remaining
        self._flush()

    # ---------------------------------------------------------------------------
    # Internal
    # ---------------------------------------------------------------------------
    def _flush(self) -> None:
        """Write current state to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        write_config(self._data, self._path)


def config_settings():
    """A utility function to load and return configurations.

    This function serves as a wrapper for loading application
    settings or configurations using the load_settings method.
    It operates internally and is not intended to be accessed
    outside the module.

    :return: Loaded application settings or configurations.
    """
    return load_settings()


class ReportConfig:
    """Wraps reports_config.yaml with typed accessors and write methods.

    Same pattern as SourceConfig — loads on construction, flushes on write.

    Usage:
        config = ReportConfig(rep_cfg_path)
        report = config.get("daily_sales_validation")
        config.add({...})
    """

    def __init__(self, path: "str | PathLike") -> None:
        self._path = Path(path)
        self._data: dict = load_config(self._path) if self._path.exists() else {}

    # ---------------------------------------------------------------------------
    # Read
    # ---------------------------------------------------------------------------

    def all(self) -> list[dict]:
        """Return all report definitions."""
        return list(self._data.get("reports", []))

    def names(self) -> list[str]:
        """Return all report names."""
        return [r["name"] for r in self.all()]

    def get(self, name: str) -> dict | None:
        """Return the report definition for a given name, or None."""
        return next((r for r in self.all() if r["name"] == name), None)

    def get_by_table(self, table: str) -> list[dict]:
        """Return all reports whose source table matches."""
        return [
            r for r in self.all()
            if r.get("source", {}).get("table") == table
        ]

    # ---------------------------------------------------------------------------
    # Write
    # ---------------------------------------------------------------------------

    def add(self, report: dict) -> None:
        """Add a new report entry and write to disk.

        :raises ValueError: If a report with the same name already exists.
        """
        if report["name"] in self.names():
            raise ValueError(
                f"Report '{report['name']}' already exists. Use update() to modify it."
            )
        reports = self.all()
        reports.append(report)
        self._data["reports"] = reports
        self._flush()

    def update(self, name: str, report: dict) -> None:
        """Replace an existing report entry and write to disk.

        :raises ValueError: If no report with that name exists.
        """
        reports = self.all()
        idx = next((i for i, r in enumerate(reports) if r["name"] == name), None)
        if idx is None:
            raise ValueError(f"No report named '{name}' found.")
        reports[idx] = report
        self._data["reports"] = reports
        self._flush()

    def add_or_update(self, report: dict) -> None:
        """Add the report if it doesn't exist, otherwise update it."""
        if report["name"] in self.names():
            self.update(report["name"], report)
        else:
            self.add(report)

    def remove(self, name: str) -> None:
        """Remove a report entry and write to disk.

        :raises ValueError: If no report with that name exists.
        """
        reports = self.all()
        remaining = [r for r in reports if r["name"] != name]
        if len(remaining) == len(reports):
            raise ValueError(f"No report named '{name}' found.")
        self._data["reports"] = remaining
        self._flush()

    # ---------------------------------------------------------------------------
    # Internal
    # ---------------------------------------------------------------------------

    def _flush(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        write_config(self._data, self._path)


class DeliverableConfig:
    """Wraps deliverables_config.yaml with typed accessors and write methods.

    Same pattern as SourceConfig and ReportConfig.

    Usage:
        config = DeliverableConfig(del_cfg_path)
        deliverable = config.get("carrier_a")
        config.add({...})
    """

    def __init__(self, path: "str | PathLike") -> None:
        self._path = Path(path)
        self._data: dict = load_config(self._path) if self._path.exists() else {}

    # ---------------------------------------------------------------------------
    # Read
    # ---------------------------------------------------------------------------

    def all(self) -> list[dict]:
        """Return all deliverable definitions."""
        return list(self._data.get("deliverables", []))

    def names(self) -> list[str]:
        """Return all deliverable names."""
        return [d["name"] for d in self.all()]

    def get(self, name: str) -> dict | None:
        """Return the deliverable definition for a given name, or None."""
        return next((d for d in self.all() if d["name"] == name), None)

    def get_by_report(self, report_name: str) -> list[dict]:
        """Return all deliverables that reference a given report."""
        return [
            d for d in self.all()
            if any(r.get("name") == report_name for r in d.get("reports", []))
        ]

    # ---------------------------------------------------------------------------
    # Write
    # ---------------------------------------------------------------------------

    def add(self, deliverable: dict) -> None:
        """Add a new deliverable entry and write to disk.

        :raises ValueError: If a deliverable with the same name already exists.
        """
        if deliverable["name"] in self.names():
            raise ValueError(
                f"Deliverable '{deliverable['name']}' already exists. "
                f"Use update() to modify it."
            )
        deliverables = self.all()
        deliverables.append(deliverable)
        self._data["deliverables"] = deliverables
        self._flush()

    def update(self, name: str, deliverable: dict) -> None:
        """Replace an existing deliverable entry and write to disk.

        :raises ValueError: If no deliverable with that name exists.
        """
        deliverables = self.all()
        idx = next(
            (i for i, d in enumerate(deliverables) if d["name"] == name), None
        )
        if idx is None:
            raise ValueError(f"No deliverable named '{name}' found.")
        deliverables[idx] = deliverable
        self._data["deliverables"] = deliverables
        self._flush()

    def add_or_update(self, deliverable: dict) -> None:
        """Add the deliverable if it doesn't exist, otherwise update it."""
        if deliverable["name"] in self.names():
            self.update(deliverable["name"], deliverable)
        else:
            self.add(deliverable)

    def remove(self, name: str) -> None:
        """Remove a deliverable entry and write to disk.

        :raises ValueError: If no deliverable with that name exists.
        """
        deliverables = self.all()
        remaining = [d for d in deliverables if d["name"] != name]
        if len(remaining) == len(deliverables):
            raise ValueError(f"No deliverable named '{name}' found.")
        self._data["deliverables"] = remaining
        self._flush()

    # ---------------------------------------------------------------------------
    # Internal
    # ---------------------------------------------------------------------------

    def _flush(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        write_config(self._data, self._path)


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


def load_config(config_path: str | PathLike) -> dict:
    """loads and returns a YAML config file, based on the given path

    :param config_path: on the path to the config file you want to load
    :return: the config dict
    """
    with open(config_path) as f:
        yaml = YAML()
        return yaml.load(f) or {}


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
    merged = _DEFAULTS.copy()

    # Merge paths sub-dict explicitly so defaults fill any missing keys
    merged["paths"].update(loaded.get("paths", {}))

    # Merge all other top-level keys generically — any new key in pipeline.yaml
    # (e.g. custom_checks_module, macros_dir) is picked up without code changes
    for key, value in loaded.items():
        if key != "paths":
            merged[key] = value
            
    # Resolve all relative paths against pipeline.yaml's own directory,
    # not the CWD — so vp works correctly from any subdirectory.
    for key, val in merged["paths"].items():
        if val and not Path(val).is_absolute():
            merged["paths"][key] = str((config_dir / val).resolve())
    return merged


def write_config(config: dict, config_path: str | PathLike) -> None:
    """Writes a YAML config file, based on the given path and config dict"""
    with open(config_path, "w") as f:
        yaml = YAML()
        yaml.dump(config, f)


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
    raw = load_config(path) if path.exists() else {}
    raw.setdefault("paths", {})[key] = value
    save_settings(raw, path)


