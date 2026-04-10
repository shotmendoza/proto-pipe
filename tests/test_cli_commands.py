"""CLI integration tests using Click's CliRunner.

Tests invoke commands directly without a real terminal and assert on:
  - Exit codes
  - Output messages
  - Side effects (config files updated, DB tables modified)

Patching strategy:
  - config_path_or_override → patched in the command module's namespace
  - load_settings → patched at proto_pipe.io.config.load_settings (local import)
  - click.confirm → bypassed by passing input="y\\n" to runner.invoke
  - _show_or_export → patched to no-op (avoids rich pager in tests)
"""

import duckdb
import pytest
from click.testing import CliRunner
from pathlib import Path
from unittest.mock import patch

from proto_pipe.io.config import SourceConfig, ReportConfig, DeliverableConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _settings(tmp_path, pipeline_db) -> dict:
    return {
        "paths": {
            "pipeline_db": pipeline_db,
            "sources_config": str(tmp_path / "sources_config.yaml"),
            "reports_config": str(tmp_path / "reports_config.yaml"),
            "deliverables_config": str(tmp_path / "deliverables_config.yaml"),
            "incoming_dir": str(tmp_path / "incoming"),
            "output_dir": str(tmp_path / "output"),
            "sql_dir": str(tmp_path / "sql"),
        }
    }


# ---------------------------------------------------------------------------
# Local fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_with_pipeline_tables(populated_db) -> str:
    from proto_pipe.io.db import init_all_pipeline_tables

    with duckdb.connect(populated_db) as conn:
        init_all_pipeline_tables(conn)
    return populated_db


@pytest.fixture()
def src_cfg_path(tmp_path) -> Path:
    path = tmp_path / "sources_config.yaml"
    config = SourceConfig(path)
    config.add(
        {
            "name": "sales",
            "target_table": "sales",
            "primary_key": "order_id",
            "patterns": ["sales*.csv"],
        }
    )
    return path


@pytest.fixture()
def rep_cfg_path(tmp_path) -> Path:
    path = tmp_path / "reports_config.yaml"
    config = ReportConfig(path)
    config.add(
        {
            "name": "daily_sales_validation",
            "source": {"table": "sales", "primary_key": "order_id"},
            "target_table": "daily_sales_validation",
            "checks": [],
        }
    )
    return path


@pytest.fixture()
def del_cfg_path(tmp_path) -> Path:
    path = tmp_path / "deliverables_config.yaml"
    config = DeliverableConfig(path)
    config.add(
        {
            "name": "monthly_sales_pack",
            "format": "csv",
            "reports": [
                {"name": "daily_sales_validation"},
            ],
        }
    )
    return path


# ---------------------------------------------------------------------------
# vp delete source
# ---------------------------------------------------------------------------


class TestDeleteSource:
    def _cfg(self, pipeline_db, src_cfg_path, rep_cfg_path, del_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "sources_config": str(src_cfg_path),
            "reports_config": str(rep_cfg_path),
            "deliverables_config": str(del_cfg_path),
        }.get(key, override or key)

    def test_removes_table(
        self,
        tmp_path,
        db_with_pipeline_tables,
        src_cfg_path,
        rep_cfg_path,
        del_cfg_path,
    ):
        from proto_pipe.cli.commands.delete import delete_source

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=self._cfg(
                db_with_pipeline_tables, src_cfg_path, rep_cfg_path, del_cfg_path
            ),
        ):
            result = runner.invoke(delete_source, ["--table", "sales", "--yes"])

        assert result.exit_code == 0, result.output

        with duckdb.connect(db_with_pipeline_tables) as conn:
            tables = conn.execute("SHOW TABLES").df()["name"].tolist()
        assert "sales" not in tables

    def test_confirms_before_delete(
        self,
        tmp_path,
        db_with_pipeline_tables,
        src_cfg_path,
        rep_cfg_path,
        del_cfg_path,
    ):
        from proto_pipe.cli.commands.delete import delete_source

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=self._cfg(
                db_with_pipeline_tables, src_cfg_path, rep_cfg_path, del_cfg_path
            ),
        ):
            result = runner.invoke(delete_source, ["--table", "sales"], input="n\n")

        with duckdb.connect(db_with_pipeline_tables) as conn:
            tables = conn.execute("SHOW TABLES").df()["name"].tolist()
        assert "sales" in tables


class TestDeleteReport:
    def _cfg(self, pipeline_db, src_cfg_path, rep_cfg_path, del_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "sources_config": str(src_cfg_path),
            "reports_config": str(rep_cfg_path),
            "deliverables_config": str(del_cfg_path),
        }.get(key, override or key)

    def test_remove_from_config(
        self,
        tmp_path,
        db_with_pipeline_tables,
        src_cfg_path,
        rep_cfg_path,
        del_cfg_path,
    ):
        from proto_pipe.cli.commands.delete import delete_report

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=self._cfg(
                db_with_pipeline_tables, src_cfg_path, rep_cfg_path, del_cfg_path
            ),
        ):
            result = runner.invoke(
                delete_report, ["--report", "daily_sales_validation", "--yes"]
            )

        assert result.exit_code == 0, result.output
        assert "daily_sales_validation" not in ReportConfig(rep_cfg_path).names()


class TestDeleteDeliverable:
    def _cfg(self, pipeline_db, del_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "deliverables_config": str(del_cfg_path),
        }.get(key, override or key)

    def test_remove_from_config(
        self, tmp_path, db_with_pipeline_tables, del_cfg_path
    ):
        from proto_pipe.cli.commands.delete import delete_deliverable

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, del_cfg_path),
        ):
            result = runner.invoke(
                delete_deliverable, ["--deliverable", "monthly_sales_pack", "--yes"]
            )

        assert result.exit_code == 0, result.output
        assert "monthly_sales_pack" not in DeliverableConfig(del_cfg_path).names()


# ---------------------------------------------------------------------------
# vp view table
# ---------------------------------------------------------------------------


class TestViewTable:
    def _cfg(self, pipeline_db, src_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "sources_config": str(src_cfg_path),
        }.get(key, override or key)

    def test_table_not_found_shows_error(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_table

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
        ):
            result = runner.invoke(view_table, ["nonexistent"])

        assert result.exit_code == 0
        assert "[error]" in result.output

    def test_displays_table_data(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_table

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
        ), patch("proto_pipe.cli.commands.view._show_or_export"):
            result = runner.invoke(view_table, ["sales"])

        assert result.exit_code == 0

    def test_interactive_select_when_no_table_name(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_table

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
        ), patch(
            "proto_pipe.cli.commands.view.questionary"
        ) as mock_q:
            mock_q.select.return_value.ask.return_value = "sales"
            mock_q.Separator = lambda x: x
            with patch("proto_pipe.cli.commands.view._show_or_export"):
                result = runner.invoke(view_table, [])

        assert result.exit_code == 0

    def test_export_csv_writes_file(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_table

        settings = {
            "paths": {
                "pipeline_db": db_with_pipeline_tables,
                "output_dir": str(tmp_path / "output"),
            }
        }

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
        ), patch(
            "proto_pipe.io.config.load_settings", return_value=settings
        ):
            result = runner.invoke(view_table, ["sales", "--export", "csv"])

        assert result.exit_code == 0
        assert "[ok]" in result.output

        output_dir = tmp_path / "output"
        csv_files = list(output_dir.glob("*.csv"))
        assert len(csv_files) == 1, f"Expected one CSV, got: {csv_files}"

    def test_export_custom_prompts_for_path(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_table

        custom_path = tmp_path / "custom_output.csv"

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
        ), patch(
            "proto_pipe.cli.prompts.prompt_custom_export_path",
            return_value=custom_path,
        ):
            result = runner.invoke(view_table, ["sales", "--export", "custom"])

        assert result.exit_code == 0
        assert custom_path.exists(), "Custom path should be written"

    def test_empty_table_shows_message(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_table

        with duckdb.connect(db_with_pipeline_tables) as conn:
            conn.execute("CREATE TABLE empty_table (id VARCHAR)")

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
        ):
            result = runner.invoke(view_table, ["empty_table"])

        assert result.exit_code == 0
        assert "empty" in result.output.lower()

    def test_limit_option_caps_rows(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_table

        captured: dict = {}

        def capture(df, title, export, pk_col=None):
            captured["df"] = df

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
        ), patch(
            "proto_pipe.cli.commands.view._show_or_export", side_effect=capture
        ):
            runner.invoke(view_table, ["sales", "--limit", "1"])

        assert "df" in captured
        assert len(captured["df"]) <= 1


# ---------------------------------------------------------------------------
# vp view source
# ---------------------------------------------------------------------------


class TestViewSource:
    def _cfg(self, pipeline_db, src_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "sources_config": str(src_cfg_path),
        }.get(key, override or key)

    def test_displays_source_data(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_source

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
        ), patch("proto_pipe.cli.commands.view._show_or_export"):
            result = runner.invoke(view_source, ["sales"])

        assert result.exit_code == 0

    def test_unknown_source_shows_error(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_source

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
        ):
            result = runner.invoke(view_source, ["nonexistent_table"])

        assert result.exit_code == 0
        assert "[error]" in result.output


# ---------------------------------------------------------------------------
# vp view report
# ---------------------------------------------------------------------------


class TestViewReport:
    def _cfg(self, pipeline_db, src_cfg_path, rep_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "sources_config": str(src_cfg_path),
            "reports_config": str(rep_cfg_path),
        }.get(key, override or key)

    def test_displays_report_source_data(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path, rep_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_report

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(
                db_with_pipeline_tables, src_cfg_path, rep_cfg_path
            ),
        ), patch("proto_pipe.cli.commands.view._show_or_export"):
            result = runner.invoke(view_report, ["daily_sales_validation"])

        assert result.exit_code == 0

    def test_unknown_report_shows_error(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path, rep_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_report

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(
                db_with_pipeline_tables, src_cfg_path, rep_cfg_path
            ),
        ):
            result = runner.invoke(view_report, ["nonexistent_report"])

        assert result.exit_code == 0
        assert "[error]" in result.output


# ---------------------------------------------------------------------------
# vp view deliverable
# ---------------------------------------------------------------------------


class TestViewDeliverable:
    def _cfg(self, pipeline_db, del_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "deliverables_config": str(del_cfg_path),
        }.get(key, override or key)

    def test_unknown_deliverable_shows_error(
        self, tmp_path, db_with_pipeline_tables, del_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_deliverable

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, del_cfg_path),
        ):
            result = runner.invoke(view_deliverable, ["nonexistent"])

        assert result.exit_code == 0
        assert "[error]" in result.output


# ---------------------------------------------------------------------------
# Spec behavioral guarantee tests
# ---------------------------------------------------------------------------


class TestDeleteSourceClearsState:
    """vp delete source clears source table, ingest_state, source_block, source_pass.

    Spec guarantee:
      'vp delete source drops the source table and cleans up all state
       tables: ingest_state, source_block, source_pass. No orphaned state
       for a deleted source.'
    """

    def _cfg(self, pipeline_db, src_cfg_path, rep_cfg_path, del_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "sources_config": str(src_cfg_path),
            "reports_config": str(rep_cfg_path),
            "deliverables_config": str(del_cfg_path),
        }.get(key, override or key)

    def test_clears_all_state_tables(
        self,
        tmp_path,
        db_with_pipeline_tables,
        src_cfg_path,
        rep_cfg_path,
        del_cfg_path,
    ):
        from proto_pipe.cli.commands.delete import delete_source
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        with duckdb.connect(db_with_pipeline_tables) as conn:
            conn.execute(
                "INSERT INTO ingest_state "
                "(id, filename, table_name, status, rows, message, ingested_at) "
                "VALUES ('s1', 'sales.csv', 'sales', 'ok', 3, NULL, ?)",
                [now],
            )
            conn.execute(
                "INSERT INTO source_block (id, table_name, check_name, "
                "pk_value, reason, flagged_at) VALUES "
                "('b1', 'sales', 'null_check', 'ORD-001', 'test', ?)",
                [now],
            )
            conn.execute(
                "INSERT INTO source_pass (pk_value, table_name, row_hash, "
                "source_file, ingested_at) VALUES "
                "('ORD-001', 'sales', 'hash1', 'sales.csv', ?)",
                [now],
            )

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=self._cfg(
                db_with_pipeline_tables, src_cfg_path, rep_cfg_path, del_cfg_path
            ),
        ):
            result = runner.invoke(delete_source, ["--table", "sales", "--yes"])

        assert result.exit_code == 0, result.output

        with duckdb.connect(db_with_pipeline_tables) as conn:
            tables = conn.execute("SHOW TABLES").df()["name"].tolist()
            assert "sales" not in tables, "Source table must be dropped"

            ingest_count = conn.execute(
                "SELECT count(*) FROM ingest_state WHERE table_name = 'sales'"
            ).fetchone()[0]
            assert ingest_count == 0, "ingest_state must be cleared"

            block_count = conn.execute(
                "SELECT count(*) FROM source_block WHERE table_name = 'sales'"
            ).fetchone()[0]
            assert block_count == 0, "source_block must be cleared"

            pass_count = conn.execute(
                "SELECT count(*) FROM source_pass WHERE table_name = 'sales'"
            ).fetchone()[0]
            assert pass_count == 0, "source_pass must be cleared"


class TestDeleteReportClearsState:
    """vp delete report clears report table, validation_block, validation_pass.

    Spec guarantee:
      'vp delete report drops the report table and cleans up all state
       tables: validation_block, validation_pass. No orphaned validation
       state for a deleted report.'
    """

    def _cfg(self, pipeline_db, src_cfg_path, rep_cfg_path, del_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "sources_config": str(src_cfg_path),
            "reports_config": str(rep_cfg_path),
            "deliverables_config": str(del_cfg_path),
        }.get(key, override or key)

    def test_clears_all_state_tables(
        self,
        tmp_path,
        db_with_pipeline_tables,
        src_cfg_path,
        rep_cfg_path,
        del_cfg_path,
    ):
        from proto_pipe.cli.commands.delete import delete_report
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        with duckdb.connect(db_with_pipeline_tables) as conn:
            conn.execute(
                "CREATE TABLE daily_sales_validation AS SELECT * FROM sales"
            )
            conn.execute(
                "INSERT INTO validation_block (id, table_name, report_name, "
                "check_name, pk_value, reason, flagged_at) VALUES "
                "('vb1', 'daily_sales_validation', 'daily_sales_validation', "
                "'range_check', 'ORD-001', 'test', ?)",
                [now],
            )
            conn.execute(
                "INSERT INTO validation_pass (pk_value, table_name, report_name, "
                "row_hash, check_set_hash, status, validated_at) VALUES "
                "('ORD-001', 'daily_sales_validation', 'daily_sales_validation', "
                "'h1', 'ch1', 'passed', ?)",
                [now],
            )

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=self._cfg(
                db_with_pipeline_tables, src_cfg_path, rep_cfg_path, del_cfg_path
            ),
        ):
            result = runner.invoke(
                delete_report, ["--report", "daily_sales_validation", "--yes"]
            )

        assert result.exit_code == 0, result.output

        with duckdb.connect(db_with_pipeline_tables) as conn:
            tables = conn.execute("SHOW TABLES").df()["name"].tolist()
            assert (
                "daily_sales_validation" not in tables
            ), "Report table must be dropped"

            block_count = conn.execute(
                "SELECT count(*) FROM validation_block "
                "WHERE report_name = 'daily_sales_validation'"
            ).fetchone()[0]
            assert block_count == 0, "validation_block must be cleared"

            pass_count = conn.execute(
                "SELECT count(*) FROM validation_pass "
                "WHERE report_name = 'daily_sales_validation'"
            ).fetchone()[0]
            assert pass_count == 0, "validation_pass must be cleared"


# ---------------------------------------------------------------------------
# TestFlaggedClear, TestFlaggedClearSourceTableUnchanged — MIGRATED
# to test_vp_errors_guarantees.py as TestVpErrorsSourceClear.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# TestViewLog — REMOVED
# vp view log removed; replaced by vp status --log.
# ---------------------------------------------------------------------------


class TestPromptCustomExportPath:
    """prompt_custom_export_path behavioral guarantees.

    Guarantees:
    - Returns None when the user cancels (empty input)
    - Returns a resolved Path when a path is provided
    """

    def test_returns_none_on_cancel(self):
        from proto_pipe.cli.prompts import prompt_custom_export_path
        import questionary

        with patch.object(
            questionary, "text", return_value=type("Q", (), {"ask": lambda self: ""})()
        ):
            result = prompt_custom_export_path()

        assert result is None

    def test_returns_resolved_path(self, tmp_path):
        from proto_pipe.cli.prompts import prompt_custom_export_path
        import questionary

        path_str = str(tmp_path / "output.csv")
        with patch.object(
            questionary,
            "text",
            return_value=type("Q", (), {"ask": lambda self: path_str})(),
        ):
            result = prompt_custom_export_path()

        assert result == Path(path_str)


class TestQueryPipelineEvents:
    """query_pipeline_events behavioral guarantees.

    Guarantees:
    - Returns all events when no filters are applied
    - severity filter returns matching rows only
    - since filter excludes events before the given date
    - order_desc=True returns most recent first (DESC)
    - order_desc=False returns chronological order (ASC)
    - Raises ValueError for a malformed since date
    - Combined severity + since filters both apply
    """

    @pytest.fixture()
    def conn_with_events(self, tmp_path):
        from datetime import datetime, timezone
        from proto_pipe.io.db import init_all_pipeline_tables

        db_path = str(tmp_path / "pipeline.db")
        conn = duckdb.connect(db_path)
        init_all_pipeline_tables(conn)
        conn.execute(
            """
            INSERT INTO pipeline_events
                (event_type, source_name, severity, detail, occurred_at)
            VALUES
                ('ingest_ok',         'sales',       'info',  '',         ?),
                ('validation_failed', 'daily_sales', 'warn',  '',         ?),
                ('ingest_failed',     'inventory',   'error', 'bad file', ?)
            """,
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 2, 1, tzinfo=timezone.utc),
                datetime(2026, 3, 1, tzinfo=timezone.utc),
            ],
        )
        yield conn
        conn.close()

    def test_returns_all_events_with_no_filters(self, conn_with_events):
        from proto_pipe.pipelines.query import query_pipeline_events

        df = query_pipeline_events(conn_with_events, severity=None, since=None)
        assert len(df) == 3

    def test_severity_filter_returns_matching_only(self, conn_with_events):
        from proto_pipe.pipelines.query import query_pipeline_events

        df = query_pipeline_events(conn_with_events, severity="error", since=None)
        assert len(df) == 1
        assert df.iloc[0]["severity"] == "error"

    def test_since_filter_excludes_earlier_events(self, conn_with_events):
        from proto_pipe.pipelines.query import query_pipeline_events

        df = query_pipeline_events(conn_with_events, severity=None, since="2026-02-01")
        assert len(df) == 2, "Events before 2026-02-01 must be excluded"

    def test_order_desc_true_returns_most_recent_first(self, conn_with_events):
        from proto_pipe.pipelines.query import query_pipeline_events

        df = query_pipeline_events(
            conn_with_events, severity=None, since=None, order_desc=True
        )
        dates = df["occurred_at"].tolist()
        assert dates == sorted(dates, reverse=True), (
            "order_desc=True must return most recent first"
        )

    def test_order_desc_false_returns_chronological(self, conn_with_events):
        from proto_pipe.pipelines.query import query_pipeline_events

        df = query_pipeline_events(
            conn_with_events, severity=None, since=None, order_desc=False
        )
        dates = df["occurred_at"].tolist()
        assert dates == sorted(dates), (
            "order_desc=False must return chronological order"
        )

    def test_malformed_since_raises_value_error(self, conn_with_events):
        from proto_pipe.pipelines.query import query_pipeline_events

        with pytest.raises(ValueError):
            query_pipeline_events(
                conn_with_events, severity=None, since="not-a-date"
            )

    def test_combined_severity_and_since_filters(self, conn_with_events):
        from proto_pipe.pipelines.query import query_pipeline_events

        df = query_pipeline_events(
            conn_with_events, severity="warn", since="2026-02-01"
        )
        assert len(df) == 1
        assert df.iloc[0]["severity"] == "warn", (
            "Combined filters must apply both severity and since constraints"
        )


from datetime import datetime, timezone


class TestPromptDeleteImpact:
    """prompt_delete_impact behavioral guarantees.

    Guarantees:
    - Returns True immediately when yes=True, prints nothing
    - Returns True when user confirms
    - Returns False when user cancels
    - Displays 'This will remove:' header before rows
    - Each row shows label, count (comma-formatted), and unit
    """

    SAMPLE_ROWS = [
        ("table 'sales'", 1234, "rows"),
        ("ingest_state", 15, "entries"),
        ("source_block", 3, "open flags"),
        ("source_pass", 1234, "entries"),
    ]

    def test_yes_returns_true_without_output(self, capsys):
        from proto_pipe.cli.prompts import prompt_delete_impact

        result = prompt_delete_impact(self.SAMPLE_ROWS, yes=True)

        assert result is True
        captured = capsys.readouterr()
        assert captured.out == "", "yes=True must print nothing"

    def test_confirmed_returns_true(self):
        from proto_pipe.cli.prompts import prompt_delete_impact

        with patch("proto_pipe.cli.prompts.click.confirm"):
            result = prompt_delete_impact(self.SAMPLE_ROWS, yes=False)

        assert result is True

    def test_cancelled_returns_false(self):
        from proto_pipe.cli.prompts import prompt_delete_impact
        import click as _click

        with patch(
            "proto_pipe.cli.prompts.click.confirm",
            side_effect=_click.Abort(),
        ):
            result = prompt_delete_impact(self.SAMPLE_ROWS, yes=False)

        assert result is False

    def test_shows_this_will_remove_header(self, capsys):
        from proto_pipe.cli.prompts import prompt_delete_impact
        import click as _click

        with patch(
            "proto_pipe.cli.prompts.click.confirm",
            side_effect=_click.Abort(),
        ):
            prompt_delete_impact(self.SAMPLE_ROWS, yes=False)

        captured = capsys.readouterr()
        assert "This will remove:" in captured.out

    def test_shows_all_row_labels(self, capsys):
        from proto_pipe.cli.prompts import prompt_delete_impact
        import click as _click

        with patch(
            "proto_pipe.cli.prompts.click.confirm",
            side_effect=_click.Abort(),
        ):
            prompt_delete_impact(self.SAMPLE_ROWS, yes=False)

        captured = capsys.readouterr()
        assert "table 'sales'" in captured.out
        assert "ingest_state" in captured.out
        assert "source_block" in captured.out
        assert "source_pass" in captured.out

    def test_count_is_comma_formatted(self, capsys):
        from proto_pipe.cli.prompts import prompt_delete_impact
        import click as _click

        with patch(
            "proto_pipe.cli.prompts.click.confirm",
            side_effect=_click.Abort(),
        ):
            prompt_delete_impact(self.SAMPLE_ROWS, yes=False)

        captured = capsys.readouterr()
        assert "1,234" in captured.out, "Counts >= 1000 must be comma-formatted"


class TestDeleteImpactQueries:
    """query_delete_*_impact behavioral guarantees.

    Guarantees:
    - Returns correct counts for each affected table
    - Returns 0 for missing tables — no error on fresh/partial DB
    - Returns list of (label, count, unit) tuples
    """

    @pytest.fixture()
    def impact_db(self, tmp_path) -> str:
        from proto_pipe.io.db import init_all_pipeline_tables

        db_path = str(tmp_path / "pipeline.db")
        now = datetime.now(timezone.utc)
        with duckdb.connect(db_path) as conn:
            init_all_pipeline_tables(conn)
            conn.execute(
                "CREATE TABLE sales (order_id VARCHAR, price DOUBLE)"
            )
            conn.execute("INSERT INTO sales VALUES ('ORD-001', 99.99)")
            conn.execute("INSERT INTO sales VALUES ('ORD-002', 250.00)")
            conn.execute(
                "INSERT INTO ingest_state "
                "(id, filename, table_name, status, rows, message, ingested_at) "
                "VALUES ('s1', 'f.csv', 'sales', 'ok', 2, NULL, ?)",
                [now],
            )
            conn.execute(
                "INSERT INTO source_block (id, table_name, check_name, "
                "pk_value, reason, flagged_at) VALUES "
                "('b1', 'sales', 'type_conflict', 'ORD-001', 'bad', ?)",
                [now],
            )
            conn.execute(
                "INSERT INTO source_pass (pk_value, table_name, row_hash, "
                "source_file, ingested_at) VALUES "
                "('ORD-001', 'sales', 'h1', 'f.csv', ?)",
                [now],
            )
            conn.execute(
                "INSERT INTO validation_block (id, table_name, report_name, "
                "check_name, pk_value, reason, flagged_at) VALUES "
                "('vb1', 'sales', 'daily_sales_validation', "
                "'range_check', 'ORD-001', 'bad', ?)",
                [now],
            )
            conn.execute(
                "INSERT INTO validation_pass (pk_value, table_name, report_name, "
                "row_hash, check_set_hash, status, validated_at) VALUES "
                "('ORD-001', 'sales', 'daily_sales_validation', "
                "'h1', 'ch1', 'passed', ?)",
                [now],
            )
        return db_path

    def test_source_impact_returns_correct_counts(self, impact_db):
        from proto_pipe.pipelines.query import query_delete_source_impact

        with duckdb.connect(impact_db) as conn:
            rows = query_delete_source_impact(conn, "sales")

        counts = {label: count for label, count, unit in rows}
        assert counts["table 'sales'"] == 2
        assert counts["ingest_state"] == 1
        assert counts["source_block"] == 1
        assert counts["source_pass"] == 1

    def test_source_impact_returns_zero_for_missing_table(self, impact_db):
        from proto_pipe.pipelines.query import query_delete_source_impact

        with duckdb.connect(impact_db) as conn:
            rows = query_delete_source_impact(conn, "nonexistent_table")

        counts = {label: count for label, count, unit in rows}
        assert all(v == 0 for v in counts.values()), (
            "Missing tables must return 0, not raise an error"
        )

    def test_source_impact_returns_tuple_list(self, impact_db):
        from proto_pipe.pipelines.query import query_delete_source_impact

        with duckdb.connect(impact_db) as conn:
            rows = query_delete_source_impact(conn, "sales")

        assert isinstance(rows, list)
        assert all(len(r) == 3 for r in rows), "Each row must be (label, count, unit)"

    def test_report_impact_returns_correct_counts(self, impact_db):
        from proto_pipe.pipelines.query import query_delete_report_impact

        with duckdb.connect(impact_db) as conn:
            rows = query_delete_report_impact(
                conn, "daily_sales_validation", "sales"
            )

        counts = {label: count for label, count, unit in rows}
        assert counts["table 'sales'"] == 2
        assert counts["validation_block"] == 1
        assert counts["validation_pass"] == 1

    def test_report_impact_returns_zero_for_missing_table(self, impact_db):
        from proto_pipe.pipelines.query import query_delete_report_impact

        with duckdb.connect(impact_db) as conn:
            rows = query_delete_report_impact(
                conn, "nonexistent_report", "nonexistent_table"
            )

        counts = {label: count for label, count, unit in rows}
        assert all(v == 0 for v in counts.values())

    def test_table_impact_returns_row_count(self, impact_db):
        from proto_pipe.pipelines.query import query_delete_table_impact

        with duckdb.connect(impact_db) as conn:
            rows = query_delete_table_impact(conn, "sales")

        assert len(rows) == 1
        label, count, unit = rows[0]
        assert count == 2
        assert unit == "rows"

    def test_table_impact_returns_zero_for_missing_table(self, impact_db):
        from proto_pipe.pipelines.query import query_delete_table_impact

        with duckdb.connect(impact_db) as conn:
            rows = query_delete_table_impact(conn, "nonexistent")

        _, count, _ = rows[0]
        assert count == 0


class TestDeleteImpactQueriesAcrossSchemas:
    """query_delete_*_impact works regardless of source table column structure.

    Guarantees:
    - Impact counts are correct when the source table has a different schema
      (different column names, more columns, fewer columns)
    - Impact counts are correct when report name differs from table name
    - Returns 0 for a table that exists but has zero rows (not just missing tables)
    """

    @pytest.fixture()
    def multi_schema_db(self, tmp_path) -> str:
        """DB with two source tables that have completely different schemas."""
        from proto_pipe.io.db import init_all_pipeline_tables

        db_path = str(tmp_path / "pipeline.db")
        now = datetime.now(timezone.utc)
        with duckdb.connect(db_path) as conn:
            init_all_pipeline_tables(conn)

            # Source 1: insurance-style schema
            conn.execute(
                "CREATE TABLE policies "
                "(policy_ref VARCHAR, carrier VARCHAR, premium DOUBLE, "
                "inception_date DATE, expiry_date DATE, status VARCHAR)"
            )
            conn.execute(
                "INSERT INTO policies VALUES "
                "('POL-001', 'Carrier A', 5000.00, '2026-01-01', '2027-01-01', 'active')"
            )
            conn.execute(
                "INSERT INTO policies VALUES "
                "('POL-002', 'Carrier B', 12000.00, '2026-03-01', '2027-03-01', 'active')"
            )
            conn.execute(
                "INSERT INTO policies VALUES "
                "('POL-003', 'Carrier A', 750.00, '2026-06-01', '2027-06-01', 'lapsed')"
            )

            # Source 2: completely different schema — flat claims data
            conn.execute(
                "CREATE TABLE claims "
                "(claim_id INTEGER, loss_date DATE, paid_amount DOUBLE)"
            )
            conn.execute(
                "INSERT INTO claims VALUES (1, '2026-02-15', 3200.00)"
            )

            # Flags for policies
            conn.execute(
                "INSERT INTO source_block (id, table_name, check_name, "
                "pk_value, reason, flagged_at) VALUES "
                "('bp1', 'policies', 'type_conflict', 'POL-001', 'bad type', ?), "
                "('bp2', 'policies', 'type_conflict', 'POL-002', 'bad type', ?)",
                [now, now],
            )
            conn.execute(
                "INSERT INTO source_pass (pk_value, table_name, row_hash, "
                "source_file, ingested_at) VALUES "
                "('POL-003', 'policies', 'h1', 'policies.csv', ?)",
                [now],
            )

            # Flags for claims
            conn.execute(
                "INSERT INTO source_block (id, table_name, check_name, "
                "pk_value, reason, flagged_at) VALUES "
                "('bc1', 'claims', 'type_conflict', '1', 'bad type', ?)",
                [now],
            )

            # Validation state: report name differs from table name
            conn.execute(
                "INSERT INTO validation_block (id, table_name, report_name, "
                "check_name, pk_value, reason, flagged_at) VALUES "
                "('vb1', 'policies', 'policy_validation_q1', "
                "'range_check', 'POL-001', 'premium out of range', ?), "
                "('vb2', 'policies', 'policy_validation_q1', "
                "'range_check', 'POL-002', 'premium out of range', ?)",
                [now, now],
            )
            conn.execute(
                "INSERT INTO validation_pass (pk_value, table_name, report_name, "
                "row_hash, check_set_hash, status, validated_at) VALUES "
                "('POL-003', 'policies', 'policy_validation_q1', "
                "'h1', 'ch1', 'passed', ?)",
                [now],
            )

        return db_path

    def test_source_impact_correct_for_wide_schema_table(self, multi_schema_db):
        """Source table with 6 columns — count query must not care about schema."""
        from proto_pipe.pipelines.query import query_delete_source_impact

        with duckdb.connect(multi_schema_db) as conn:
            rows = query_delete_source_impact(conn, "policies")

        counts = {label: count for label, count, unit in rows}
        assert counts["table 'policies'"] == 3, (
            "Row count must reflect actual rows regardless of column structure"
        )
        assert counts["source_block"] == 2
        assert counts["source_pass"] == 1

    def test_source_impact_correct_for_narrow_schema_table(self, multi_schema_db):
        """Source table with 3 columns — count query must not care about schema."""
        from proto_pipe.pipelines.query import query_delete_source_impact

        with duckdb.connect(multi_schema_db) as conn:
            rows = query_delete_source_impact(conn, "claims")

        counts = {label: count for label, count, unit in rows}
        assert counts["table 'claims'"] == 1
        assert counts["source_block"] == 1
        assert counts["source_pass"] == 0

    def test_report_impact_correct_when_report_name_differs_from_table(
        self, multi_schema_db
    ):
        """Report name 'policy_validation_q1' is different from table 'policies'."""
        from proto_pipe.pipelines.query import query_delete_report_impact

        with duckdb.connect(multi_schema_db) as conn:
            rows = query_delete_report_impact(
                conn, "policy_validation_q1", "policies"
            )

        counts = {label: count for label, count, unit in rows}
        assert counts["table 'policies'"] == 3
        assert counts["validation_block"] == 2, (
            "validation_block count must be scoped to report_name, not table_name"
        )
        assert counts["validation_pass"] == 1

    def test_source_impact_returns_zero_rows_for_empty_table(self, multi_schema_db):
        """A table that exists but has zero rows must return 0, not an error."""
        from proto_pipe.pipelines.query import query_delete_source_impact

        with duckdb.connect(multi_schema_db) as conn:
            conn.execute(
                "CREATE TABLE empty_source (ref VARCHAR, amount DOUBLE)"
            )
            rows = query_delete_source_impact(conn, "empty_source")

        counts = {label: count for label, count, unit in rows}
        assert counts["table 'empty_source'"] == 0, (
            "Empty table must return 0 rows, not an error"
        )


class TestIngestProgressReporter:
    """IngestProgressReporter.on_file_done behavioral guarantees.

    Guarantees:
    - Uses self._progress.console.print for all output — never click.echo.
      click.echo conflicts with the active rich progress context and would
      be overwritten by the spinner.
    - ok: prints filename and row count; includes flagged count when nonzero.
    - failed: prints filename and error message.
    - skipped: prints filename.
    - Counters (_ok, _failed, _skipped) are incremented correctly.
    """

    def _make_reporter(self):
        """Return a reporter with _progress mocked — no real terminal needed."""
        from proto_pipe.cli.prompts import IngestProgressReporter
        from unittest.mock import MagicMock

        reporter = IngestProgressReporter()
        reporter._progress = MagicMock()
        reporter._task = MagicMock()
        return reporter

    def test_ok_uses_console_print_not_click_echo(self):
        reporter = self._make_reporter()

        with patch("click.echo") as mock_echo:
            reporter.on_file_done(
                "sales_2026.csv", {"status": "ok", "rows": 100, "flagged": 0}
            )

        assert reporter._progress.console.print.called, (
            "on_file_done must use self._progress.console.print, not click.echo"
        )
        assert not mock_echo.called, (
            "click.echo must not be called inside on_file_done — "
            "it conflicts with the active rich progress bar"
        )

    def test_ok_output_contains_filename(self):
        reporter = self._make_reporter()
        reporter.on_file_done(
            "sales_2026.csv", {"status": "ok", "rows": 1000, "flagged": 0}
        )

        printed = str(reporter._progress.console.print.call_args)
        assert "sales_2026.csv" in printed

    def test_ok_output_contains_row_count(self):
        reporter = self._make_reporter()
        reporter.on_file_done(
            "sales_2026.csv", {"status": "ok", "rows": 1000, "flagged": 0}
        )

        printed = str(reporter._progress.console.print.call_args)
        assert "1000" in printed, "Row count must appear in the ok result line"

    def test_ok_output_contains_flagged_count_when_nonzero(self):
        reporter = self._make_reporter()
        reporter.on_file_done(
            "sales_2026.csv", {"status": "ok", "rows": 500, "flagged": 3}
        )

        printed = str(reporter._progress.console.print.call_args)
        assert "3" in printed, (
            "Flagged count must appear in output when nonzero so user knows to check source_block"
        )

    def test_ok_output_omits_flagged_when_zero(self):
        reporter = self._make_reporter()
        reporter.on_file_done(
            "sales_2026.csv", {"status": "ok", "rows": 500, "flagged": 0}
        )

        printed = str(reporter._progress.console.print.call_args)
        assert "blocked" not in printed, (
            "Flagged/blocked count must not appear in output when zero"
        )

    def test_failed_uses_console_print(self):
        reporter = self._make_reporter()
        reporter.on_file_done(
            "bad.csv", {"status": "failed", "rows": 0, "message": "type mismatch"}
        )

        assert reporter._progress.console.print.called
        printed = str(reporter._progress.console.print.call_args)
        assert "bad.csv" in printed
        assert "type mismatch" in printed

    def test_skipped_uses_console_print(self):
        reporter = self._make_reporter()
        reporter.on_file_done("already_loaded.csv", {"status": "skipped"})

        assert reporter._progress.console.print.called
        printed = str(reporter._progress.console.print.call_args)
        assert "already_loaded.csv" in printed

    def test_ok_increments_ok_counter(self):
        reporter = self._make_reporter()
        reporter.on_file_done("f.csv", {"status": "ok", "rows": 10, "flagged": 0})
        assert reporter._ok == 1

    def test_failed_increments_failed_counter(self):
        reporter = self._make_reporter()
        reporter.on_file_done("f.csv", {"status": "failed", "rows": 0, "message": "err"})
        assert reporter._failed == 1

    def test_skipped_increments_skipped_counter(self):
        reporter = self._make_reporter()
        reporter.on_file_done("f.csv", {"status": "skipped"})
        assert reporter._skipped == 1
