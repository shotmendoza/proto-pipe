"""Behavioral guarantee tests for vp new view.

Each test maps to one user-observable guarantee. Tests assert on
end-state (config file contents, SQL file contents, CLI output) — not on
whether internal functions were called.

Mocking strategy:
  - ViewConfigPrompter.run() → mocked to return True and populate
    .view_config / .sql_spec / .insert_after without real terminal prompts
  - load_settings → returns temp pipeline.yaml paths
  - config_path_or_override → routes to temp paths
  - get_all_source_tables → returns fake table list (no real DB needed
    for wiring tests)
  - load_views_config → returns existing view list from temp config
  - Write path (write_config, file write) → NEVER mocked (Rule: never
    mock the write path)
"""

import pytest
from click.testing import CliRunner
from pathlib import Path
from unittest.mock import patch, MagicMock

from proto_pipe.io.config import load_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(tmp_path, sql_dir, views_cfg_path):
    """Build a settings dict pointing at temp paths."""
    return {
        "paths": {
            "pipeline_db": str(tmp_path / "pipeline.db"),
            "sources_config": str(tmp_path / "sources_config.yaml"),
            "reports_config": str(tmp_path / "reports_config.yaml"),
            "deliverables_config": str(tmp_path / "deliverables_config.yaml"),
            "views_config": str(views_cfg_path),
            "incoming_dir": str(tmp_path / "incoming"),
            "output_dir": str(tmp_path / "output"),
            "sql_dir": str(sql_dir),
        }
    }


def _make_mock_prompter(view_name, base_table, view_type,
                        group_by_columns=None, aggregate_functions=None,
                        where_clause="", insert_after=None):
    """Return a mock ViewConfigPrompter class whose run() returns True."""
    from proto_pipe.cli.scaffold import ViewSQLSpec

    spec = ViewSQLSpec(
        view_name=view_name,
        base_table=base_table,
        view_type=view_type,
        group_by_columns=group_by_columns or [],
        aggregate_functions=aggregate_functions or {},
        where_clause=where_clause,
    )

    mock_cls = MagicMock()
    instance = MagicMock()
    instance.run.return_value = True
    instance.sql_spec = spec
    instance.view_config = {"name": view_name, "sql_file": ""}
    instance.insert_after = insert_after
    mock_cls.return_value = instance
    return mock_cls


def _make_cancelled_prompter():
    """Return a mock ViewConfigPrompter class whose run() returns False."""
    mock_cls = MagicMock()
    instance = MagicMock()
    instance.run.return_value = False
    mock_cls.return_value = instance
    return mock_cls


FAKE_TABLES = [("premiums_report", 1), ("claims_report", 0)]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sql_dir(tmp_path) -> Path:
    d = tmp_path / "sql"
    d.mkdir()
    return d


@pytest.fixture()
def views_cfg_path(tmp_path) -> Path:
    """Empty views config file."""
    path = tmp_path / "views_config.yaml"
    from ruamel.yaml import YAML
    yaml = YAML()
    with open(path, "w") as f:
        yaml.dump({"views": []}, f)
    return path


@pytest.fixture()
def views_cfg_with_existing(tmp_path) -> Path:
    """Views config with one existing view."""
    path = tmp_path / "views_config.yaml"
    from ruamel.yaml import YAML
    yaml = YAML()
    with open(path, "w") as f:
        yaml.dump({"views": [
            {"name": "base_view", "sql_file": "sql/base_view.sql"},
        ]}, f)
    return path


# ---------------------------------------------------------------------------
# Shared runner
# ---------------------------------------------------------------------------

def _run_new_view(tmp_path, sql_dir, views_cfg_path, mock_prompter_cls):
    """Invoke vp new view with mocked prompts, return CliRunner result."""
    from proto_pipe.cli.commands.new import new_view

    settings = _make_settings(tmp_path, sql_dir, views_cfg_path)
    rep_cfg_path = Path(settings["paths"]["reports_config"])
    if not rep_cfg_path.exists():
        from ruamel.yaml import YAML
        yaml = YAML()
        with open(rep_cfg_path, "w") as f:
            yaml.dump({"reports": []}, f)

    runner = CliRunner()
    with (
        patch("proto_pipe.cli.commands.new.load_settings",
              return_value=settings),
        patch("proto_pipe.cli.commands.new.config_path_or_override",
              side_effect=lambda key, override=None: override or settings["paths"].get(key, key)),
        patch("proto_pipe.cli.commands.new.get_all_source_tables",
              return_value=FAKE_TABLES),
        patch("proto_pipe.cli.commands.new.ViewConfigPrompter",
              mock_prompter_cls),
    ):
        result = runner.invoke(new_view, [])

    return result


# ---------------------------------------------------------------------------
# Behavioral Guarantee Tests
# ---------------------------------------------------------------------------

class TestVpNewViewGuarantees:
    """Each test verifies one user-observable guarantee of vp new view."""

    # ----- Guarantee 1: Aggregate SQL has GROUP BY and aggregate functions -----

    def test_aggregate_sql_has_group_by(self, tmp_path, sql_dir, views_cfg_path):
        """Aggregate view produces SQL with GROUP BY and aggregate functions."""
        mock_prompter = _make_mock_prompter(
            view_name="carrier_summary",
            base_table="premiums_report",
            view_type="aggregate",
            group_by_columns=["carrier"],
            aggregate_functions={"bound_premium": "SUM", "policy_id": "COUNT"},
        )

        result = _run_new_view(tmp_path, sql_dir, views_cfg_path, mock_prompter)

        assert result.exit_code == 0, result.output
        sql_file = sql_dir / "carrier_summary.sql"
        assert sql_file.exists()
        sql = sql_file.read_text()
        assert "GROUP BY carrier" in sql
        assert "SUM(bound_premium)" in sql
        assert "COUNT(policy_id)" in sql

    # ----- Guarantee 2: Filter SQL has WHERE clause -----

    def test_filter_sql_has_where_clause(self, tmp_path, sql_dir, views_cfg_path):
        """Filter view produces SQL with the user's WHERE condition."""
        mock_prompter = _make_mock_prompter(
            view_name="active_policies",
            base_table="premiums_report",
            view_type="filter",
            where_clause="status = 'active'",
        )

        result = _run_new_view(tmp_path, sql_dir, views_cfg_path, mock_prompter)

        assert result.exit_code == 0, result.output
        sql_file = sql_dir / "active_policies.sql"
        assert sql_file.exists()
        sql = sql_file.read_text()
        assert "WHERE status = 'active'" in sql

    # ----- Guarantee 3: View registered in views_config.yaml -----

    def test_view_registered_in_config(self, tmp_path, sql_dir, views_cfg_path):
        """After wizard completes, the view entry exists in views_config.yaml."""
        mock_prompter = _make_mock_prompter(
            view_name="carrier_summary",
            base_table="premiums_report",
            view_type="custom",
        )

        result = _run_new_view(tmp_path, sql_dir, views_cfg_path, mock_prompter)

        assert result.exit_code == 0, result.output
        config = load_config(views_cfg_path)
        view_names = [v["name"] for v in config.get("views", [])]
        assert "carrier_summary" in view_names
        # sql_file path is set
        entry = [v for v in config["views"] if v["name"] == "carrier_summary"][0]
        assert entry["sql_file"] != ""

    # ----- Guarantee 4: Cancel = no config writes, no SQL file -----

    def test_cancel_produces_no_writes(self, tmp_path, sql_dir, views_cfg_path):
        """If the user cancels, no config is written and no SQL file is created."""
        mock_prompter = _make_cancelled_prompter()

        result = _run_new_view(tmp_path, sql_dir, views_cfg_path, mock_prompter)

        assert "Cancelled" in result.output
        config = load_config(views_cfg_path)
        assert len(config.get("views", [])) == 0
        sql_files = list(sql_dir.glob("*.sql"))
        assert len(sql_files) == 0

    # ----- Guarantee 5: Duplicate name → prompter handles re-prompt -----

    def test_duplicate_name_handled_by_prompter(
        self, tmp_path, sql_dir, views_cfg_with_existing
    ):
        """When a view name already exists, the prompter's prompt_name
        asks for confirmation. If the user declines (run returns False),
        no writes happen."""
        mock_prompter = _make_cancelled_prompter()

        result = _run_new_view(
            tmp_path, sql_dir, views_cfg_with_existing, mock_prompter
        )

        assert "Cancelled" in result.output
        config = load_config(views_cfg_with_existing)
        view_names = [v["name"] for v in config.get("views", [])]
        assert view_names == ["base_view"]  # unchanged

    # ----- Guarantee 6: Dependency → inserted after referenced view -----

    def test_dependency_inserted_after_referenced_view(
        self, tmp_path, sql_dir, views_cfg_with_existing
    ):
        """When insert_after is set, the new view appears after the
        referenced view in views_config.yaml."""
        mock_prompter = _make_mock_prompter(
            view_name="derived_view",
            base_table="premiums_report",
            view_type="custom",
            insert_after="base_view",
        )

        result = _run_new_view(
            tmp_path, sql_dir, views_cfg_with_existing, mock_prompter
        )

        assert result.exit_code == 0, result.output
        config = load_config(views_cfg_with_existing)
        view_names = [v["name"] for v in config.get("views", [])]
        assert view_names == ["base_view", "derived_view"]
        # base_view is at index 0, derived_view at index 1 (after base_view)
        base_idx = view_names.index("base_view")
        derived_idx = view_names.index("derived_view")
        assert derived_idx == base_idx + 1
