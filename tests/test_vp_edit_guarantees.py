"""Behavioral guarantee tests for vp edit deliverable.

Tests what the user observes: config updates, SQL file regeneration,
cancel behavior, SQL overwrite confirmation, rename handling.

Mock strategy: interactive prompts only. Never mock the write path
(DeliverableConfig.add_or_update, Path.write_text).
"""

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import yaml

from proto_pipe.io.config import DeliverableConfig, load_config


@pytest.fixture
def edit_env(tmp_path):
    """Pipeline environment with one existing deliverable."""
    import duckdb

    pipeline_yaml = tmp_path / "pipeline.yaml"
    pipeline_yaml.write_text(yaml.dump({
        "paths": {
            "pipeline_db": str(tmp_path / "pipeline.db"),
            "incoming_dir": str(tmp_path / "incoming"),
            "output_dir": str(tmp_path / "output"),
            "sql_dir": str(tmp_path / "sql"),
            "sources_config": str(tmp_path / "sources_config.yaml"),
            "reports_config": str(tmp_path / "reports_config.yaml"),
            "deliverables_config": str(tmp_path / "deliverables_config.yaml"),
        },
    }))

    sources_cfg = tmp_path / "sources_config.yaml"
    sources_cfg.write_text(yaml.dump({"sources": [
        {"name": "sales", "target_table": "sales", "primary_key": "id",
         "patterns": ["sales_*.csv"]},
    ]}))

    reports_cfg = tmp_path / "reports_config.yaml"
    reports_cfg.write_text(yaml.dump({"reports": [
        {"name": "premiums_report", "source": {"type": "duckdb", "table": "sales"}},
        {"name": "claims_report", "source": {"type": "duckdb", "table": "sales"}},
    ]}))

    sql_dir = tmp_path / "sql"
    sql_dir.mkdir()

    # Existing SQL file
    existing_sql = sql_dir / "carrier_a.sql"
    existing_sql.write_text("-- hand-edited SQL\nSELECT * FROM premiums_report")

    # Existing deliverable in config
    del_cfg = tmp_path / "deliverables_config.yaml"
    del_cfg.write_text(yaml.dump({"deliverables": [
        {
            "name": "carrier_a",
            "format": "xlsx",
            "filename_template": "carrier_a_{date}.xlsx",
            "sql_file": str(existing_sql),
            "reports": [{"name": "premiums_report"}],
        },
    ]}))

    # Create pipeline DB with report tables
    db_path = str(tmp_path / "pipeline.db")
    conn = duckdb.connect(db_path)
    conn.execute(
        "CREATE TABLE premiums_report ("
        "  policy_id VARCHAR, carrier VARCHAR, bound_premium DOUBLE,"
        "  _ingested_at TIMESTAMP, _row_hash VARCHAR"
        ")"
    )
    conn.execute(
        "CREATE TABLE claims_report ("
        "  policy_id VARCHAR, carrier VARCHAR, claim_amount DOUBLE,"
        "  _ingested_at TIMESTAMP, _row_hash VARCHAR"
        ")"
    )
    conn.close()

    return {
        "tmp_path": tmp_path,
        "del_cfg": str(del_cfg),
        "rep_cfg": str(reports_cfg),
        "src_cfg": str(sources_cfg),
        "sql_dir": str(sql_dir),
        "pipeline_db": db_path,
        "existing_sql": str(existing_sql),
    }


def _run_edit_wizard(edit_env, prompt_values, existing_deliverable=None):
    """Helper: construct prompter in edit mode and run with mocked prompts."""
    from proto_pipe.cli.prompts import DeliverableConfigPrompter

    rep_config = load_config(edit_env["rep_cfg"])
    src_config = load_config(edit_env["src_cfg"])

    if existing_deliverable is None:
        config = DeliverableConfig(edit_env["del_cfg"])
        existing_deliverable = config.get("carrier_a")

    prompter = DeliverableConfigPrompter(
        rep_config=rep_config,
        src_config=src_config,
        sql_dir=edit_env["sql_dir"],
        existing_deliverable=existing_deliverable,
    )

    prompt_returns = iter(prompt_values)

    with patch("proto_pipe.cli.prompts.questionary") as mock_q:
        mock_q.text.return_value.ask = lambda: next(prompt_returns)
        mock_q.select.return_value.ask = lambda: next(prompt_returns)
        mock_q.checkbox.return_value.ask = lambda: next(prompt_returns)
        mock_q.confirm.return_value.ask = lambda: next(prompt_returns)
        mock_q.Choice = MagicMock(side_effect=lambda *a, **kw: kw.get("value", a[0]))

        result = prompter.run(
            existing_names=[],
            available_reports=["premiums_report", "claims_report"],
            pipeline_db=edit_env["pipeline_db"],
        )

    return result, prompter


class TestVpEditDeliverableGuarantees:
    """Behavioral guarantees for vp edit deliverable."""

    def test_edit_preserves_name_when_unchanged(self, edit_env):
        """Editing without changing the name keeps the same config entry."""
        result, prompter = _run_edit_wizard(edit_env, [
            "carrier_a",                              # name (unchanged)
            "xlsx",                                   # format
            "carrier_a_{date}.xlsx",                  # filename template
            ["premiums_report"],                      # reports
            ["policy_id", "carrier", "bound_premium"],  # columns
            False,                                    # order by? no
            True,                                     # confirm SQL overwrite
            False,                                    # fill details? no
        ])

        assert result is True
        assert prompter.deliverable["name"] == "carrier_a"

        # Write and verify
        config = DeliverableConfig(edit_env["del_cfg"])
        config.add_or_update(prompter.deliverable)

        saved = load_config(edit_env["del_cfg"])
        names = [d["name"] for d in saved.get("deliverables", [])]
        assert names.count("carrier_a") == 1

    def test_edit_with_rename_removes_old_adds_new(self, edit_env):
        """Renaming a deliverable removes the old entry and adds the new one."""
        result, prompter = _run_edit_wizard(edit_env, [
            "carrier_b",                              # name (renamed)
            "csv",                                    # format
            "carrier_b_{date}.csv",                   # filename template
            ["premiums_report"],                      # reports
            ["policy_id", "bound_premium"],            # columns
            False,                                    # order by? no
            True,                                     # confirm SQL overwrite
            False,                                    # fill details? no
        ])

        assert result is True
        assert prompter.deliverable["name"] == "carrier_b"

        # Simulate edit command's rename logic
        config = DeliverableConfig(edit_env["del_cfg"])
        config.remove("carrier_a")
        config.add_or_update(prompter.deliverable)

        saved = load_config(edit_env["del_cfg"])
        names = [d["name"] for d in saved.get("deliverables", [])]
        assert "carrier_a" not in names
        assert "carrier_b" in names

    def test_sql_file_regenerated_with_selected_columns(self, edit_env):
        """After edit, the SQL file is regenerated with the wizard-selected columns."""
        result, prompter = _run_edit_wizard(edit_env, [
            "carrier_a",                              # name
            "xlsx",                                   # format
            "carrier_a_{date}.xlsx",                  # filename template
            ["premiums_report"],                      # reports
            ["policy_id", "bound_premium"],            # columns (subset)
            False,                                    # order by? no
            True,                                     # confirm SQL overwrite
            False,                                    # fill details? no
        ])

        assert result is True
        sql_path = Path(edit_env["sql_dir"]) / "carrier_a.sql"
        content = sql_path.read_text()

        # Regenerated — no longer the hand-edited content
        assert "hand-edited" not in content
        assert "policy_id" in content
        assert "bound_premium" in content

    def test_cancel_at_name_produces_no_changes(self, edit_env):
        """Cancelling at the name prompt leaves config and SQL file unchanged."""
        original_sql = Path(edit_env["existing_sql"]).read_text()
        original_config = load_config(edit_env["del_cfg"])

        from proto_pipe.cli.prompts import DeliverableConfigPrompter

        rep_config = load_config(edit_env["rep_cfg"])
        src_config = load_config(edit_env["src_cfg"])
        config = DeliverableConfig(edit_env["del_cfg"])
        existing = config.get("carrier_a")

        prompter = DeliverableConfigPrompter(
            rep_config=rep_config,
            src_config=src_config,
            sql_dir=edit_env["sql_dir"],
            existing_deliverable=existing,
        )

        with patch("proto_pipe.cli.prompts.questionary") as mock_q:
            mock_q.text.return_value.ask = lambda: None  # Cancel

            result = prompter.run(
                existing_names=[],
                available_reports=["premiums_report"],
                pipeline_db=edit_env["pipeline_db"],
            )

        assert result is False
        assert prompter.deliverable == {}
        assert Path(edit_env["existing_sql"]).read_text() == original_sql
        assert load_config(edit_env["del_cfg"]) == original_config

    def test_sql_overwrite_declined_keeps_old_sql(self, edit_env):
        """Declining SQL overwrite confirmation preserves the original SQL file."""
        original_sql = Path(edit_env["existing_sql"]).read_text()

        result, prompter = _run_edit_wizard(edit_env, [
            "carrier_a",                              # name
            "xlsx",                                   # format
            "carrier_a_{date}.xlsx",                  # filename template
            ["premiums_report"],                      # reports
            ["policy_id", "bound_premium"],            # columns
            False,                                    # order by? no
            False,                                    # DECLINE SQL overwrite
            False,                                    # fill details? no
        ])

        assert result is True
        # SQL file unchanged
        assert Path(edit_env["existing_sql"]).read_text() == original_sql
        # Config still references the old SQL file
        assert prompter.deliverable["sql_file"] == edit_env["existing_sql"]
