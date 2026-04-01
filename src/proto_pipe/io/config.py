"""SourceConfig — owns all read/write/query operations against sources_config.yaml.

All code that previously did raw dict access on the sources config
(load_config / iterate sources / source.get("primary_key")) should use
this class instead.
"""
from __future__ import annotations

from os import PathLike
from pathlib import Path


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
        from proto_pipe.io.registry import load_config
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
        from proto_pipe.io.registry import write_config
        self._path.parent.mkdir(parents=True, exist_ok=True)
        write_config(self._data, self._path)
