"""Tests for proto_pipe.io.config.load_settings

Covers the behavioral guarantee documented in CLAUDE.md:
  Relative paths in pipeline.yaml resolve against the config file's own
  directory, not CWD. So `vp` works correctly from any subdirectory.
"""

from pathlib import Path

import pytest

from proto_pipe.io.config import load_settings


# ---------------------------------------------------------------------------
# Relative path resolution
# ---------------------------------------------------------------------------

class TestLoadSettingsRelativePathResolution:
    def test_relative_paths_resolve_against_config_dir_not_cwd(self, tmp_path):
        """Relative paths in pipeline.yaml must resolve against the file's
        own directory, not the process CWD.

        Documented guarantee in CLAUDE.md:
          'Relative paths are resolved against pipeline.yaml's own directory,
           not CWD — so vp works correctly from any subdirectory.'
        """
        # Place pipeline.yaml in a subdirectory — NOT the CWD
        config_dir = tmp_path / "project"
        config_dir.mkdir()
        pipeline_yaml = config_dir / "pipeline.yaml"
        pipeline_yaml.write_text(
            "paths:\n"
            "  pipeline_db: \"data/pipeline.db\"\n"
            "  incoming_dir: \"data/incoming/\"\n"
        )

        settings = load_settings(pipeline_yaml)

        pipeline_db = Path(settings["paths"]["pipeline_db"])
        incoming_dir = Path(settings["paths"]["incoming_dir"])

        # Must resolve relative to config_dir, not CWD
        assert pipeline_db == (config_dir / "data/pipeline.db").resolve()
        assert incoming_dir == (config_dir / "data/incoming/").resolve()

        # Must NOT resolve relative to CWD
        assert pipeline_db != (Path.cwd() / "data/pipeline.db").resolve()

    def test_absolute_paths_are_left_unchanged(self, tmp_path):
        """Absolute paths must pass through unmodified."""
        config_dir = tmp_path / "project"
        config_dir.mkdir()
        pipeline_yaml = config_dir / "pipeline.yaml"
        abs_path = str(tmp_path / "shared" / "pipeline.db")
        pipeline_yaml.write_text(
            f"paths:\n"
            f"  pipeline_db: \"{abs_path}\"\n"
        )

        settings = load_settings(pipeline_yaml)

        assert settings["paths"]["pipeline_db"] == abs_path


    def test_all_relative_path_keys_resolved(self, tmp_path):
        """Every relative path key in pipeline.yaml is resolved, not just some."""
        config_dir = tmp_path / "project"
        config_dir.mkdir()
        pipeline_yaml = config_dir / "pipeline.yaml"
        pipeline_yaml.write_text(
            "paths:\n"
            "  pipeline_db: \"data/pipeline.db\"\n"
            "  watermark_db: \"data/watermarks.db\"\n"
            "  incoming_dir: \"data/incoming/\"\n"
            "  output_dir: \"output/reports/\"\n"
            "  sources_config: \"config/sources_config.yaml\"\n"
        )

        settings = load_settings(pipeline_yaml)

        for key, val in settings["paths"].items():
            if val:
                # Every resolved path must be absolute
                assert Path(val).is_absolute(), (
                    f"Path '{key}' = '{val}' is not absolute after resolution"
                )
                # Every resolved path must be under config_dir (not CWD)
                assert str(val).startswith(str(config_dir.resolve())), (
                    f"Path '{key}' = '{val}' did not resolve against config_dir"
                )

    def test_missing_pipeline_yaml_returns_defaults(self, tmp_path):
        """Missing pipeline.yaml returns defaults without raising."""
        non_existent = tmp_path / "pipeline.yaml"
        settings = load_settings(non_existent)
        assert "paths" in settings
        assert "pipeline_db" in settings["paths"]

        # Mutating the returned dict must not affect _DEFAULTS
        original_value = settings["paths"]["pipeline_db"]
        settings["paths"]["pipeline_db"] = "/mutated/path"
        fresh = load_settings(non_existent)
        assert fresh["paths"]["pipeline_db"] == original_value, (
            "Mutating returned settings dict affected _DEFAULTS — "
            "load_settings must return a deep copy"
        )
