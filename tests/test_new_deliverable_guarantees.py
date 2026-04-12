"""Behavioral guarantee tests for vp new deliverable.

Tests what the user observes: config writes, SQL file creation,
cancel behavior, overwrite confirmation.

Mock strategy: interactive prompts only. Never mock the write path
(DeliverableConfig.add_or_update, Path.write_text).
"""

import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import yaml
from click.testing import CliRunner


@pytest.fixture
def deliverable_env(tmp_path):
    """Set up a minimal pipeline environment for deliverable tests."""
    import duckdb

    # Config files
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

    del_cfg = tmp_path / "deliverables_config.yaml"
    del_cfg.write_text(yaml.dump({"deliverables": []}))

    sql_dir = tmp_path / "sql"
    sql_dir.mkdir()

    # Create a pipeline DB with report tables
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
        "pipeline_yaml": str(pipeline_yaml),
    }


class TestVpNewDeliverableGuarantees:
    """Behavioral guarantees for vp new deliverable."""

    def test_wizard_writes_deliverable_to_config(self, deliverable_env):
        """A completed wizard run adds the deliverable to deliverables_config.yaml."""
        from proto_pipe.cli.prompts import DeliverableConfigPrompter
        from proto_pipe.io.config import DeliverableConfig, load_config

        rep_config = load_config(deliverable_env["rep_cfg"])
        src_config = load_config(deliverable_env["src_cfg"])

        prompter = DeliverableConfigPrompter(
            rep_config=rep_config,
            src_config=src_config,
            sql_dir=deliverable_env["sql_dir"],
        )

        # Mock all questionary prompts in sequence
        prompt_returns = iter([
            "carrier_a",                  # name
            "xlsx",                       # format
            "carrier_a_{date}.xlsx",      # filename template
            ["premiums_report"],          # reports
            ["policy_id", "carrier", "bound_premium"],  # columns for premiums_report
            False,                        # order by? no
            False,                        # fill details? no
        ])

        with patch("proto_pipe.cli.prompts.questionary") as mock_q:
            # Each .ask() returns next value
            mock_q.text.return_value.ask = lambda: next(prompt_returns)
            mock_q.select.return_value.ask = lambda: next(prompt_returns)
            mock_q.checkbox.return_value.ask = lambda: next(prompt_returns)
            mock_q.confirm.return_value.ask = lambda: next(prompt_returns)
            mock_q.Choice = MagicMock(side_effect=lambda *a, **kw: kw.get("value", a[0]))

            result = prompter.run(
                existing_names=[],
                available_reports=["premiums_report", "claims_report"],
                pipeline_db=deliverable_env["pipeline_db"],
            )

        assert result is True
        assert prompter.deliverable["name"] == "carrier_a"
        assert prompter.deliverable["format"] == "xlsx"

        # Write to config and verify
        config = DeliverableConfig(deliverable_env["del_cfg"])
        config.add_or_update(prompter.deliverable)

        saved = load_config(deliverable_env["del_cfg"])
        names = [d["name"] for d in saved.get("deliverables", [])]
        assert "carrier_a" in names

    def test_sql_file_created_with_correct_content(self, deliverable_env):
        """The wizard creates a SQL file with SELECT/FROM referencing the report table."""
        from proto_pipe.cli.scaffold import (
            DeliverableSQLSpec,
            build_deliverable_sql,
        )

        spec = DeliverableSQLSpec(
            deliverable_name="carrier_a",
            report_columns={"premiums_report": ["policy_id", "carrier", "bound_premium"]},
        )
        sql = build_deliverable_sql(spec)
        sql_path = Path(deliverable_env["sql_dir"]) / "carrier_a.sql"
        sql_path.write_text(sql)

        assert sql_path.exists()
        content = sql_path.read_text()
        assert "SELECT" in content
        assert "FROM premiums_report" in content
        assert "policy_id" in content

    def test_sql_file_with_join_contains_correct_tables(self, deliverable_env):
        """Multi-report SQL has JOINs with correct table names and keys."""
        from proto_pipe.cli.scaffold import (
            JoinSpec,
            DeliverableSQLSpec,
            build_deliverable_sql,
        )

        spec = DeliverableSQLSpec(
            deliverable_name="combined",
            report_columns={
                "premiums_report": ["policy_id", "bound_premium"],
                "claims_report": ["policy_id", "claim_amount"],
            },
            join_specs=[
                JoinSpec("premiums_report", "claims_report", "policy_id", "policy_id", "LEFT"),
            ],
        )
        sql = build_deliverable_sql(spec)
        sql_path = Path(deliverable_env["sql_dir"]) / "combined.sql"
        sql_path.write_text(sql)

        content = sql_path.read_text()
        assert "FROM premiums_report" in content
        assert "JOIN claims_report" in content
        assert "policy_id" in content

    def test_cancel_at_name_produces_no_config_writes(self, deliverable_env):
        """Cancelling at the name prompt leaves config and filesystem unchanged."""
        from proto_pipe.cli.prompts import DeliverableConfigPrompter
        from proto_pipe.io.config import load_config

        rep_config = load_config(deliverable_env["rep_cfg"])
        src_config = load_config(deliverable_env["src_cfg"])

        prompter = DeliverableConfigPrompter(
            rep_config=rep_config,
            src_config=src_config,
            sql_dir=deliverable_env["sql_dir"],
        )

        with patch("proto_pipe.cli.prompts.questionary") as mock_q:
            mock_q.text.return_value.ask = lambda: None  # Cancel at name

            result = prompter.run(
                existing_names=[],
                available_reports=["premiums_report"],
                pipeline_db=deliverable_env["pipeline_db"],
            )

        assert result is False
        assert prompter.deliverable == {}

        # No SQL files created
        sql_files = list(Path(deliverable_env["sql_dir"]).glob("*.sql"))
        assert len(sql_files) == 0

        # Config unchanged
        saved = load_config(deliverable_env["del_cfg"])
        assert saved.get("deliverables", []) == []

    def test_existing_name_prompts_overwrite_confirmation(self, deliverable_env):
        """When a name already exists, the wizard asks for confirmation."""
        from proto_pipe.cli.prompts import DeliverableConfigPrompter
        from proto_pipe.io.config import load_config

        rep_config = load_config(deliverable_env["rep_cfg"])
        src_config = load_config(deliverable_env["src_cfg"])

        prompter = DeliverableConfigPrompter(
            rep_config=rep_config,
            src_config=src_config,
            sql_dir=deliverable_env["sql_dir"],
        )

        # User types existing name, then declines overwrite
        call_count = {"text": 0, "confirm": 0}

        def mock_text_ask():
            call_count["text"] += 1
            return "existing_deliverable"

        def mock_confirm_ask():
            call_count["confirm"] += 1
            return False  # Decline overwrite

        with patch("proto_pipe.cli.prompts.questionary") as mock_q:
            mock_q.text.return_value.ask = mock_text_ask
            mock_q.confirm.return_value.ask = mock_confirm_ask

            result = prompter.run(
                existing_names=["existing_deliverable"],
                available_reports=["premiums_report"],
                pipeline_db=deliverable_env["pipeline_db"],
            )

        assert result is False
        # confirm was called (overwrite prompt)
        assert call_count["confirm"] >= 1

    def test_no_views_available_skips_view_step(self, deliverable_env):
        """When no views exist, wizard skips view selection and produces SQL without view JOINs."""
        from proto_pipe.cli.prompts import DeliverableConfigPrompter
        from proto_pipe.cli.scaffold import DeliverableSQLSpec, build_deliverable_sql
        from proto_pipe.io.config import load_config

        rep_config = load_config(deliverable_env["rep_cfg"])
        src_config = load_config(deliverable_env["src_cfg"])

        prompter = DeliverableConfigPrompter(
            rep_config=rep_config,
            src_config=src_config,
            sql_dir=deliverable_env["sql_dir"],
        )

        # Create an empty views config
        views_cfg = Path(deliverable_env["tmp_path"] / "views_config.yaml")
        views_cfg.write_text(yaml.dump({"views": []}))

        prompt_returns = iter([
            "no_views_test",              # name
            "xlsx",                       # format
            "no_views_{date}.xlsx",       # filename template
            ["premiums_report"],          # reports
            ["policy_id", "bound_premium"],  # columns
            False,                        # order by? no
            False,                        # fill details? no
        ])

        with patch("proto_pipe.cli.prompts.questionary") as mock_q:
            mock_q.text.return_value.ask = lambda: next(prompt_returns)
            mock_q.select.return_value.ask = lambda: next(prompt_returns)
            mock_q.checkbox.return_value.ask = lambda: next(prompt_returns)
            mock_q.confirm.return_value.ask = lambda: next(prompt_returns)
            mock_q.Choice = MagicMock(side_effect=lambda *a, **kw: kw.get("value", a[0]))

            result = prompter.run(
                existing_names=[],
                available_reports=["premiums_report"],
                pipeline_db=deliverable_env["pipeline_db"],
                views_config_path=str(views_cfg),
            )

        assert result is True

        # Read the generated SQL — should have no JOIN
        sql_path = Path(deliverable_env["sql_dir"]) / "no_views_test.sql"
        assert sql_path.exists()
        content = sql_path.read_text()
        assert "JOIN" not in content
        assert "-- Views:" not in content

    def test_view_selected_produces_sql_with_view_join(self, deliverable_env):
        """When a view is selected, SQL includes the view JOIN and view columns."""
        import duckdb
        from proto_pipe.cli.prompts import DeliverableConfigPrompter
        from proto_pipe.io.config import load_config

        # Create the view in DuckDB so get_table_columns works
        conn = duckdb.connect(deliverable_env["pipeline_db"])
        conn.execute(
            "CREATE VIEW carrier_summary AS "
            "SELECT carrier, SUM(bound_premium) AS total_premium "
            "FROM premiums_report GROUP BY carrier"
        )
        conn.close()

        # Create views config
        views_cfg = Path(deliverable_env["tmp_path"] / "views_config.yaml")
        views_cfg.write_text(yaml.dump({"views": [
            {"name": "carrier_summary", "sql_file": "carrier_summary.sql"},
        ]}))

        rep_config = load_config(deliverable_env["rep_cfg"])
        src_config = load_config(deliverable_env["src_cfg"])

        prompter = DeliverableConfigPrompter(
            rep_config=rep_config,
            src_config=src_config,
            sql_dir=deliverable_env["sql_dir"],
        )

        # Mock the full wizard flow including view steps.
        # Sequence: name, format, filename, reports, report_columns,
        # view_checkbox, view_columns, base_table_select,
        # join_left_key, join_right_key, join_type, order_by_confirm
        checkbox_calls = iter([
            ["premiums_report"],                         # reports
            ["policy_id", "carrier", "bound_premium"],   # report columns
            ["carrier_summary"],                         # views checkbox
            ["carrier", "total_premium"],                # view columns
        ])
        select_calls = iter([
            "xlsx",              # format
            "premiums_report",   # base table selection (Issue 3 — user picks left side)
            "carrier",           # join left key (from premiums_report)
            "carrier",           # join right key (from carrier_summary)
            "LEFT",              # join type
        ])
        text_calls = iter([
            "view_join_test",                # name
            "view_join_{date}.xlsx",         # filename template
        ])
        confirm_calls = iter([
            False,  # order by? no
            False,  # fill details? no
        ])

        with patch("proto_pipe.cli.prompts.questionary") as mock_q:
            mock_q.checkbox.return_value.ask = lambda: next(checkbox_calls)
            mock_q.select.return_value.ask = lambda: next(select_calls)
            mock_q.text.return_value.ask = lambda: next(text_calls)
            mock_q.confirm.return_value.ask = lambda: next(confirm_calls)
            mock_q.Choice = MagicMock(side_effect=lambda *a, **kw: kw.get("value", a[0]))

            result = prompter.run(
                existing_names=[],
                available_reports=["premiums_report"],
                pipeline_db=deliverable_env["pipeline_db"],
                views_config_path=str(views_cfg),
            )

        assert result is True

        sql_path = Path(deliverable_env["sql_dir"]) / "view_join_test.sql"
        assert sql_path.exists()
        content = sql_path.read_text()

        assert "JOIN carrier_summary" in content
        assert "carrier" in content
        assert "total_premium" in content
        assert "-- Views: carrier_summary" in content
