"""Behavioral guarantee tests for vp new source.

Each test maps to one user-observable guarantee. Tests assert on
end-state (config file contents, DB rows, CLI output) — not on
whether internal functions were called.

Mocking strategy:
  - SourceConfigPrompter.run() → mocked to return True and populate
    .source / .confirmed_types without real terminal prompts
  - config_path_or_override → routes to temp paths
  - load_settings → returns temp pipeline.yaml paths
  - DuckDB connections → real temp DB (no mock)
  - _scan_incoming / filter_unconfigured / _group_files_by_pattern →
    mocked to simulate file discovery without real files

This pattern should be replicated for all vp new/edit/delete commands.
"""

import duckdb
import pytest
from click.testing import CliRunner
from pathlib import Path
from unittest.mock import patch, MagicMock

from proto_pipe.io.config import SourceConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(tmp_path, pipeline_db, src_cfg_path, incoming_dir):
    """Build a settings dict pointing at temp paths."""
    return {
        "paths": {
            "pipeline_db": pipeline_db,
            "sources_config": str(src_cfg_path),
            "reports_config": str(tmp_path / "reports_config.yaml"),
            "deliverables_config": str(tmp_path / "deliverables_config.yaml"),
            "incoming_dir": str(incoming_dir),
            "output_dir": str(tmp_path / "output"),
            "sql_dir": str(tmp_path / "sql"),
        }
    }


FAKE_SOURCE = {
    "name": "premiums",
    "patterns": ["premiums_*.csv"],
    "target_table": "premiums",
    "primary_key": "policy_id",
    "on_duplicate": "flag",
    "timestamp_col": "updated_at",
}

FAKE_TYPES = {
    "policy_id": "VARCHAR",
    "premium": "DOUBLE",
    "updated_at": "TIMESTAMPTZ",
}


def _mock_prompter_cls(source=None, confirmed_types=None):
    """Return a mock SourceConfigPrompter class whose run() returns True.

    The mock also simulates prompt_file_group returning a selected file
    and suggested pattern, since that's called as a classmethod before
    the instance is created.
    """
    source = source or FAKE_SOURCE
    types = confirmed_types or FAKE_TYPES

    mock_cls = MagicMock()

    # Class method — called before instance creation
    mock_cls.prompt_file_group.return_value = ("premiums_2026-03.csv", "premiums_*")

    # Instance — returned by mock_cls(...)
    instance = MagicMock()
    instance.run.return_value = True
    instance.source = source
    instance.confirmed_types = types
    mock_cls.return_value = instance

    return mock_cls


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def src_cfg_path(tmp_path) -> Path:
    """Empty sources config file."""
    path = tmp_path / "sources_config.yaml"
    from ruamel.yaml import YAML
    yaml = YAML()
    with open(path, "w") as f:
        yaml.dump({"sources": []}, f)
    return path


@pytest.fixture()
def src_cfg_with_existing(tmp_path) -> Path:
    """Sources config with one existing source."""
    path = tmp_path / "sources_config.yaml"
    config = SourceConfig(path)
    config.add({
        "name": "premiums",
        "patterns": ["premiums_*.csv"],
        "target_table": "premiums",
        "primary_key": "policy_id",
        "on_duplicate": "flag",
    })
    return path


@pytest.fixture()
def incoming_dir(tmp_path) -> Path:
    d = tmp_path / "incoming"
    d.mkdir()
    return d


@pytest.fixture()
def pipeline_db_with_tables(tmp_path) -> str:
    db_path = str(tmp_path / "pipeline.db")
    from proto_pipe.io.db import init_all_pipeline_tables
    with duckdb.connect(db_path) as conn:
        init_all_pipeline_tables(conn)
    return db_path


# ---------------------------------------------------------------------------
# Behavioral Guarantee Tests
# ---------------------------------------------------------------------------

class TestVpNewSourceGuarantees:
    """Each test verifies one user-observable guarantee of vp new source."""

    def _run(self, tmp_path, pipeline_db, src_cfg_path, incoming_dir,
             source=None, confirmed_types=None):
        """Invoke vp new source with mocked prompts, return (result, config)."""
        from proto_pipe.cli.commands.new import new_source

        settings = _make_settings(tmp_path, pipeline_db, src_cfg_path, incoming_dir)
        mock_prompter = _mock_prompter_cls(source, confirmed_types)

        patches = {
            "proto_pipe.cli.commands.new.config_path_or_override":
                lambda key, override=None: settings["paths"].get(key, override or key),
            "proto_pipe.cli.commands.new.load_settings":
                lambda: settings,
            "proto_pipe.cli.commands.new.SourceConfigPrompter":
                mock_prompter,
            "proto_pipe.cli.commands.new._scan_incoming":
                lambda d: ["premiums_2026-03.csv"],
            "proto_pipe.cli.commands.new.filter_unconfigured":
                lambda files, sources: files,
            "proto_pipe.cli.commands.new._group_files_by_pattern":
                lambda files: {"premiums_*": ["premiums_2026-03.csv"]},
        }

        runner = CliRunner()
        with patch.multiple("proto_pipe.cli.commands.new", **{
            k.split(".")[-1]: v for k, v in patches.items()
        }):
            # patch.multiple uses short names — need full path patches instead
            pass

        # Use individual patches for full dotted paths
        with (
            patch("proto_pipe.cli.commands.new.config_path_or_override",
                  side_effect=lambda key, override=None: settings["paths"].get(key, override or key)),
            patch("proto_pipe.cli.commands.new.load_settings", return_value=settings),
            patch("proto_pipe.cli.commands.new.SourceConfigPrompter", mock_prompter),
            patch("proto_pipe.cli.commands.new._scan_incoming",
                  return_value=["premiums_2026-03.csv"]),
            patch("proto_pipe.cli.commands.new.filter_unconfigured",
                  side_effect=lambda files, sources: files),
            patch("proto_pipe.cli.commands.new._group_files_by_pattern",
                  return_value={"premiums_*": ["premiums_2026-03.csv"]}),
            patch("proto_pipe.cli.commands.new.duckdb") as mock_duckdb,
        ):
            # Mock the DuckDB connect used for schema preview (not pipeline tables)
            import pandas as pd
            mock_conn = MagicMock()
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_conn.execute.return_value.df.return_value = pd.DataFrame({
                "policy_id": ["POL-001"], "premium": [100.0], "updated_at": ["2026-01-01"]
            })
            mock_duckdb.connect.return_value = mock_conn

            result = runner.invoke(new_source, [])

        config = SourceConfig(src_cfg_path)
        return result, config

    # ----- Guarantee 1: Source written to config file -----

    def test_source_written_to_config_file(
        self, tmp_path, pipeline_db_with_tables, src_cfg_path, incoming_dir
    ):
        """After vp new source completes, the source entry exists in
        sources_config.yaml with the correct name and fields."""
        result, config = self._run(
            tmp_path, pipeline_db_with_tables, src_cfg_path, incoming_dir
        )

        assert result.exit_code == 0, result.output
        assert "premiums" in config.names()
        saved = config.get("premiums")
        assert saved["target_table"] == "premiums"
        assert saved["primary_key"] == "policy_id"

    # ----- Guarantee 2: OK message shows correct path -----

    def test_ok_message_shows_config_path(
        self, tmp_path, pipeline_db_with_tables, src_cfg_path, incoming_dir
    ):
        """The success message names the actual config file path."""
        result, _ = self._run(
            tmp_path, pipeline_db_with_tables, src_cfg_path, incoming_dir
        )

        assert result.exit_code == 0, result.output
        assert "[ok]" in result.output
        assert str(src_cfg_path) in result.output

    # ----- Guarantee 3: Existing source name updates, not duplicates -----

    def test_existing_name_updates_not_duplicates(
        self, tmp_path, pipeline_db_with_tables, src_cfg_with_existing, incoming_dir
    ):
        """When the source name already exists, add_or_update replaces it
        rather than creating a duplicate entry."""
        updated_source = {**FAKE_SOURCE, "on_duplicate": "upsert"}

        result, config = self._run(
            tmp_path, pipeline_db_with_tables, src_cfg_with_existing, incoming_dir,
            source=updated_source,
        )

        assert result.exit_code == 0, result.output
        # Only one entry with that name
        assert config.names().count("premiums") == 1
        # Updated field persisted
        assert config.get("premiums")["on_duplicate"] == "upsert"

    # ----- Guarantee 4: Cancel produces no config writes -----

    def test_cancel_produces_no_config_writes(
        self, tmp_path, pipeline_db_with_tables, src_cfg_path, incoming_dir
    ):
        """If the user cancels at any prompt step, the config file is unchanged."""
        from proto_pipe.cli.commands.new import new_source
        import pandas as pd

        settings = _make_settings(
            tmp_path, pipeline_db_with_tables, src_cfg_path, incoming_dir
        )

        # Prompter that returns False (user cancelled)
        mock_prompter = _mock_prompter_cls()
        mock_prompter.return_value.run.return_value = False

        runner = CliRunner()
        with (
            patch("proto_pipe.cli.commands.new.config_path_or_override",
                  side_effect=lambda key, override=None: settings["paths"].get(key, override or key)),
            patch("proto_pipe.cli.commands.new.load_settings", return_value=settings),
            patch("proto_pipe.cli.commands.new.SourceConfigPrompter", mock_prompter),
            patch("proto_pipe.cli.commands.new._scan_incoming",
                  return_value=["premiums_2026-03.csv"]),
            patch("proto_pipe.cli.commands.new.filter_unconfigured",
                  side_effect=lambda files, sources: files),
            patch("proto_pipe.cli.commands.new._group_files_by_pattern",
                  return_value={"premiums_*": ["premiums_2026-03.csv"]}),
            patch("proto_pipe.cli.commands.new.duckdb") as mock_duckdb,
        ):
            mock_conn = MagicMock()
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_conn.execute.return_value.df.return_value = pd.DataFrame({
                "policy_id": ["POL-001"], "premium": [100.0]
            })
            mock_duckdb.connect.return_value = mock_conn

            result = runner.invoke(new_source, [])

        assert "Cancelled" in result.output
        config = SourceConfig(src_cfg_path)
        assert "premiums" not in config.names()

    # ----- Guarantee 5: No files found shows error, no writes -----

    def test_no_files_shows_error_no_writes(
        self, tmp_path, pipeline_db_with_tables, src_cfg_path, incoming_dir
    ):
        """When incoming_dir has no files, an error is shown and config is untouched."""
        from proto_pipe.cli.commands.new import new_source

        settings = _make_settings(
            tmp_path, pipeline_db_with_tables, src_cfg_path, incoming_dir
        )

        runner = CliRunner()
        with (
            patch("proto_pipe.cli.commands.new.config_path_or_override",
                  side_effect=lambda key, override=None: settings["paths"].get(key, override or key)),
            patch("proto_pipe.cli.commands.new.load_settings", return_value=settings),
            patch("proto_pipe.cli.commands.new._scan_incoming", return_value=[]),
        ):
            result = runner.invoke(new_source, [])

        assert "[error]" in result.output
        config = SourceConfig(src_cfg_path)
        assert len(config.all()) == 0
