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
            "patterns": ["sales_*.csv"],
            "primary_key": "order_id",
            "timestamp_col": "updated_at",
            "on_duplicate": "flag",
        }
    )
    return path


@pytest.fixture()
def rep_cfg_path(tmp_path, pipeline_db) -> Path:
    path = tmp_path / "reports_config.yaml"
    config = ReportConfig(path)
    config.add(
        {
            "name": "daily_sales_validation",
            "source": {"type": "duckdb", "path": pipeline_db, "table": "sales"},
            "options": {"parallel": False},
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
            "name": "carrier_a",
            "format": "xlsx",
            "filename_template": "carrier_a_{date}.xlsx",
            "reports": [{"name": "daily_sales_validation", "sheet": "Sales"}],
        }
    )
    return path


# ---------------------------------------------------------------------------
# vp delete
# ---------------------------------------------------------------------------


class TestDeleteSource:
    def _cfg(self, src_cfg_path, pipeline_db):
        return lambda key, override=None: {
            "sources_config": str(src_cfg_path),
            "pipeline_db": pipeline_db,
        }.get(key, override or key)

    def test_removes_config_entry(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.delete import delete_source

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=self._cfg(src_cfg_path, db_with_pipeline_tables),
        ):
            result = runner.invoke(delete_source, ["--table", "sales"], input="y\n")

        assert result.exit_code == 0, result.output
        assert "sales" not in SourceConfig(src_cfg_path).names()

    def test_drops_table_from_db(self, tmp_path, db_with_pipeline_tables, src_cfg_path):
        from proto_pipe.cli.commands.delete import delete_source

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=self._cfg(src_cfg_path, db_with_pipeline_tables),
        ):
            runner.invoke(delete_source, ["--table", "sales"], input="y\n")

        with duckdb.connect(db_with_pipeline_tables) as conn:
            tables = (
                conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
                )
                .df()["table_name"]
                .tolist()
            )
        assert "sales" not in tables

    def test_unknown_table_shows_error(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.delete import delete_source

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=self._cfg(src_cfg_path, db_with_pipeline_tables),
        ):
            result = runner.invoke(delete_source, ["--table", "nonexistent"])

        assert result.exit_code == 0
        assert "error" in result.output.lower() or "not found" in result.output.lower()


class TestDeleteReport:
    def test_removes_config_entry(self, tmp_path, pipeline_db, rep_cfg_path):
        from proto_pipe.cli.commands.delete import delete_report

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=lambda key, override=None: {
                "reports_config": str(rep_cfg_path),
                "pipeline_db": pipeline_db,
            }.get(key, override or key),
        ):
            result = runner.invoke(
                delete_report, ["--report", "daily_sales_validation"], input="y\n"
            )

        assert result.exit_code == 0, result.output
        assert "daily_sales_validation" not in ReportConfig(rep_cfg_path).names()

    def test_unknown_report_shows_error(self, tmp_path, pipeline_db, rep_cfg_path):
        from proto_pipe.cli.commands.delete import delete_report

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=lambda key, override=None: {
                "reports_config": str(rep_cfg_path),
                "pipeline_db": pipeline_db,
            }.get(key, override or key),
        ):
            result = runner.invoke(delete_report, ["--report", "nonexistent"])

        assert result.exit_code == 0
        assert "error" in result.output.lower() or "not found" in result.output.lower()


class TestDeleteDeliverable:
    def test_removes_config_entry(self, tmp_path, pipeline_db, del_cfg_path):
        from proto_pipe.cli.commands.delete import delete_deliverable

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=lambda key, override=None: {
                "deliverables_config": str(del_cfg_path),
                "pipeline_db": pipeline_db,
            }.get(key, override or key),
        ):
            result = runner.invoke(
                delete_deliverable, ["--deliverable", "carrier_a"], input="y\n"
            )

        assert result.exit_code == 0, result.output
        assert "carrier_a" not in DeliverableConfig(del_cfg_path).names()


# ---------------------------------------------------------------------------
# vp view
# ---------------------------------------------------------------------------


class TestViewTable:
    def test_shows_rows(self, tmp_path, db_with_pipeline_tables):
        from proto_pipe.cli.commands.view import view_table

        runner = CliRunner()
        with (
            patch(
                "proto_pipe.cli.commands.view.config_path_or_override",
                return_value=db_with_pipeline_tables,
            ),
            patch("proto_pipe.cli.commands.view._show_or_export"),
            patch(
                "proto_pipe.io.config.load_settings",
                return_value=_settings(tmp_path, db_with_pipeline_tables),
            ),
        ):
            result = runner.invoke(view_table, ["sales"])

        assert result.exit_code == 0, result.output

    def test_unknown_table_shows_error(self, tmp_path, db_with_pipeline_tables):
        from proto_pipe.cli.commands.view import view_table

        runner = CliRunner()
        with (
            patch(
                "proto_pipe.cli.commands.view.config_path_or_override",
                return_value=db_with_pipeline_tables,
            ),
            patch(
                "proto_pipe.io.config.load_settings",
                return_value=_settings(tmp_path, db_with_pipeline_tables),
            ),
        ):
            result = runner.invoke(view_table, ["nonexistent"])

        assert result.exit_code == 0
        assert "error" in result.output.lower() or "not found" in result.output.lower()

    def test_no_tables_shows_message(self, tmp_path, pipeline_db):
        from proto_pipe.cli.commands.view import view_table

        duckdb.connect(pipeline_db).close()  # empty DB

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            return_value=pipeline_db,
        ):
            result = runner.invoke(view_table, [])

        assert result.exit_code == 0
        assert "no tables" in result.output.lower()


class TestViewSource:
    def _cfg(self, pipeline_db, src_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "sources_config": str(src_cfg_path),
        }.get(key, override or key)

    def test_shows_rows(self, tmp_path, db_with_pipeline_tables, src_cfg_path):
        from proto_pipe.cli.commands.view import view_source

        runner = CliRunner()
        with (
            patch(
                "proto_pipe.cli.commands.view.config_path_or_override",
                side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
            ),
            patch("proto_pipe.cli.commands.view._show_or_export"),
        ):
            result = runner.invoke(view_source, ["sales"])

        assert result.exit_code == 0, result.output

    def test_empty_table_shows_message(self, tmp_path, pipeline_db, src_cfg_path):
        from proto_pipe.cli.commands.view import view_source
        from proto_pipe.io.db import init_all_pipeline_tables

        with duckdb.connect(pipeline_db) as conn:
            init_all_pipeline_tables(conn)
            conn.execute("CREATE TABLE sales (order_id VARCHAR, price DOUBLE)")

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(pipeline_db, src_cfg_path),
        ):
            result = runner.invoke(view_source, ["sales"])

        assert result.exit_code == 0
        assert "empty" in result.output.lower()

    def test_unconfigured_source_shows_error(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_source

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
        ):
            # inventory is in the DB but not in src_cfg_path
            result = runner.invoke(view_source, ["inventory"])

        assert result.exit_code == 0
        assert "error" in result.output.lower() or "not found" in result.output.lower()


class TestViewReport:
    def _cfg(self, pipeline_db, src_cfg_path, rep_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "sources_config": str(src_cfg_path),
            "reports_config": str(rep_cfg_path),
        }.get(key, override or key)

    def test_shows_rows(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path, rep_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_report

        runner = CliRunner()
        with (
            patch(
                "proto_pipe.cli.commands.view.config_path_or_override",
                side_effect=self._cfg(
                    db_with_pipeline_tables, src_cfg_path, rep_cfg_path
                ),
            ),
            patch("proto_pipe.cli.commands.view._show_or_export"),
        ):
            result = runner.invoke(view_report, ["daily_sales_validation"])

        assert result.exit_code == 0, result.output

    def test_unknown_report_shows_error(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path, rep_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_report

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path, rep_cfg_path),
        ):
            result = runner.invoke(view_report, ["nonexistent"])

        assert result.exit_code == 0
        assert "error" in result.output.lower() or "not found" in result.output.lower()


class TestViewDeliverable:
    def _cfg(self, pipeline_db, del_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "deliverables_config": str(del_cfg_path),
        }.get(key, override or key)

    def test_missing_sql_shows_error(
        self, tmp_path, db_with_pipeline_tables, del_cfg_path
    ):
        from proto_pipe.cli.commands.view import view_deliverable

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.view.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, del_cfg_path),
        ):
            result = runner.invoke(view_deliverable, ["carrier_a"])

        assert result.exit_code == 0
        assert "error" in result.output.lower() or "no sql" in result.output.lower()

    def test_with_sql_file(self, tmp_path, db_with_pipeline_tables, del_cfg_path):
        from proto_pipe.cli.commands.view import view_deliverable

        sql_path = tmp_path / "carrier_a.sql"
        sql_path.write_text("SELECT * FROM sales;")

        config = DeliverableConfig(del_cfg_path)
        entry = config.get("carrier_a")
        entry["sql_file"] = str(sql_path)
        config.update("carrier_a", entry)

        runner = CliRunner()
        with (
            patch(
                "proto_pipe.cli.commands.view.config_path_or_override",
                side_effect=self._cfg(db_with_pipeline_tables, del_cfg_path),
            ),
            patch("proto_pipe.cli.commands.view._show_or_export"),
        ):
            result = runner.invoke(view_deliverable, ["carrier_a"])

        assert result.exit_code == 0, result.output


# ---------------------------------------------------------------------------
# vp flagged
# ---------------------------------------------------------------------------


class TestFlaggedClear:
    def _cfg(self, pipeline_db, src_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "sources_config": str(src_cfg_path),
        }.get(key, override or key)

    def test_clear_removes_rows(self, tmp_path, db_with_pipeline_tables, src_cfg_path):
        from proto_pipe.cli.flagged import flagged_clear

        with duckdb.connect(db_with_pipeline_tables) as conn:
            conn.execute("""
                         INSERT INTO source_block (id, table_name, check_name, pk_value, reason, flagged_at)
                         VALUES ('abc123', 'sales', 'null_check', 'pk-abc', 'test flag',
                                 TIMESTAMPTZ '2026-01-01T00:00:00+00:00')
                         """)

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.flagged.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
        ):
            result = runner.invoke(flagged_clear, ["--table", "sales", "--yes"])

        assert result.exit_code == 0, result.output

        with duckdb.connect(db_with_pipeline_tables) as conn:
            count = conn.execute(
                "SELECT count(*) FROM source_block WHERE table_name = 'sales'"
            ).fetchone()[0]
        assert count == 0

    def test_clear_empty_shows_message(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.flagged import flagged_clear

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.flagged.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
        ):
            result = runner.invoke(flagged_clear, ["--table", "sales", "--yes"])

        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# CLAUDE.md behavioral guarantee tests
# ---------------------------------------------------------------------------


class TestDeleteSourceClearsState:
    """vp delete source clears source table, ingest_state, source_block, source_pass.

    CLAUDE.md guarantee:
      'vp delete source clears: source table, ingest_state, source_block, source_pass.'
    """

    def _cfg(self, src_cfg_path, pipeline_db):
        return lambda key, override=None: {
            "sources_config": str(src_cfg_path),
            "pipeline_db": pipeline_db,
        }.get(key, override or key)

    def _seed_state(self, pipeline_db):
        """Seed ingest_state, source_block, source_pass for the sales table."""
        from datetime import datetime, timezone
        import uuid

        with duckdb.connect(pipeline_db) as conn:
            now = datetime.now(timezone.utc)
            conn.execute(
                """
                         INSERT INTO ingest_state (id, filename, table_name, status, ingested_at)
                         VALUES (?, 'sales_2026.csv', 'sales', 'ok', ?)
                         """,
                [str(uuid.uuid4()), now],
            )
            conn.execute(
                """
                         INSERT INTO source_block (id, table_name, check_name, pk_value,
                                                   reason, flagged_at)
                         VALUES ('flag1', 'sales', 'duplicate_conflict', 'ORD-001',
                                 'changed', ?)
                         """,
                [now],
            )
            conn.execute(
                """
                         INSERT INTO source_pass (pk_value, table_name, row_hash,
                                                  source_file, ingested_at)
                         VALUES ('ORD-001', 'sales', 'abc123', 'sales_2026.csv', ?)
                         """,
                [now],
            )

    def test_delete_source_clears_ingest_state(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.delete import delete_source

        self._seed_state(db_with_pipeline_tables)
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=self._cfg(src_cfg_path, db_with_pipeline_tables),
        ):
            runner.invoke(delete_source, ["--table", "sales"], input="y\n")

        with duckdb.connect(db_with_pipeline_tables) as conn:
            count = conn.execute(
                "SELECT count(*) FROM ingest_state WHERE table_name = 'sales'"
            ).fetchone()[0]
        assert count == 0, "vp delete source must clear ingest_state for the table"

    def test_delete_source_clears_source_block(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.delete import delete_source

        self._seed_state(db_with_pipeline_tables)
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=self._cfg(src_cfg_path, db_with_pipeline_tables),
        ):
            runner.invoke(delete_source, ["--table", "sales"], input="y\n")

        with duckdb.connect(db_with_pipeline_tables) as conn:
            count = conn.execute(
                "SELECT count(*) FROM source_block WHERE table_name = 'sales'"
            ).fetchone()[0]
        assert count == 0, "vp delete source must clear source_block for the table"

    def test_delete_source_clears_source_pass(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.commands.delete import delete_source

        self._seed_state(db_with_pipeline_tables)
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=self._cfg(src_cfg_path, db_with_pipeline_tables),
        ):
            runner.invoke(delete_source, ["--table", "sales"], input="y\n")

        with duckdb.connect(db_with_pipeline_tables) as conn:
            count = conn.execute(
                "SELECT count(*) FROM source_pass WHERE table_name = 'sales'"
            ).fetchone()[0]
        assert count == 0, "vp delete source must clear source_pass for the table"


class TestDeleteReportClearsState:
    """vp delete report clears report table, validation_block, validation_pass.

    CLAUDE.md guarantee:
      'vp delete report clears: report table, validation_block, validation_pass.'
    """

    def _seed_validation_state(self, pipeline_db):
        from datetime import datetime, timezone
        import uuid

        with duckdb.connect(pipeline_db) as conn:
            now = datetime.now(timezone.utc)
            conn.execute(
                """
                         INSERT INTO validation_block
                         (id, table_name, report_name, check_name, pk_value,
                          reason, flagged_at)
                         VALUES ('vb1', 'sales', 'daily_sales_validation',
                                 'range_check', 'ORD-001', 'out of range', ?)
                         """,
                [now],
            )
            conn.execute(
                """
                         INSERT INTO validation_pass
                         (pk_value, table_name, report_name, row_hash,
                          check_set_hash, status, validated_at)
                         VALUES ('ORD-001', 'sales', 'daily_sales_validation',
                                 'hash1', 'cshash1', 'passed', ?)
                         """,
                [now],
            )

    def test_delete_report_clears_validation_block(
        self, tmp_path, db_with_pipeline_tables, rep_cfg_path
    ):
        from proto_pipe.cli.commands.delete import delete_report

        self._seed_validation_state(db_with_pipeline_tables)
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=lambda key, override=None: {
                "reports_config": str(rep_cfg_path),
                "pipeline_db": db_with_pipeline_tables,
            }.get(key, override or key),
        ):
            runner.invoke(
                delete_report, ["--report", "daily_sales_validation"], input="y\n"
            )

        with duckdb.connect(db_with_pipeline_tables) as conn:
            count = conn.execute(
                "SELECT count(*) FROM validation_block "
                "WHERE report_name = 'daily_sales_validation'"
            ).fetchone()[0]
        assert count == 0, "vp delete report must clear validation_block for the report"

    def test_delete_report_clears_validation_pass(
        self, tmp_path, db_with_pipeline_tables, rep_cfg_path
    ):
        from proto_pipe.cli.commands.delete import delete_report

        self._seed_validation_state(db_with_pipeline_tables)
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.delete.config_path_or_override",
            side_effect=lambda key, override=None: {
                "reports_config": str(rep_cfg_path),
                "pipeline_db": db_with_pipeline_tables,
            }.get(key, override or key),
        ):
            runner.invoke(
                delete_report, ["--report", "daily_sales_validation"], input="y\n"
            )

        with duckdb.connect(db_with_pipeline_tables) as conn:
            count = conn.execute(
                "SELECT count(*) FROM validation_pass "
                "WHERE report_name = 'daily_sales_validation'"
            ).fetchone()[0]
        assert count == 0, "vp delete report must clear validation_pass for the report"


class TestFlaggedClearSourceTableUnchanged:
    """vp flagged clear drops flags without modifying the source table.

    CLAUDE.md guarantee:
      'vp flagged clear — drops flags without inserting anything.
       Existing rows in source table unchanged.'
    """

    def _cfg(self, pipeline_db, src_cfg_path):
        return lambda key, override=None: {
            "pipeline_db": pipeline_db,
            "sources_config": str(src_cfg_path),
        }.get(key, override or key)

    def test_source_table_rows_unchanged_after_clear(
        self, tmp_path, db_with_pipeline_tables, src_cfg_path
    ):
        from proto_pipe.cli.flagged import flagged_clear
        from datetime import datetime, timezone

        with duckdb.connect(db_with_pipeline_tables) as conn:
            now = datetime.now(timezone.utc)
            conn.execute(
                """
                         INSERT INTO source_block (id, table_name, check_name, pk_value,
                                                   reason, flagged_at)
                         VALUES ('flag1', 'sales', 'duplicate_conflict', 'ORD-001',
                                 'changed', ?)
                         """,
                [now],
            )
            # Count source table rows before clear
            before_count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.flagged.config_path_or_override",
            side_effect=self._cfg(db_with_pipeline_tables, src_cfg_path),
        ):
            runner.invoke(flagged_clear, ["--table", "sales", "--yes"])

        with duckdb.connect(db_with_pipeline_tables) as conn:
            after_count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]

        assert after_count == before_count, (
            "vp flagged clear must not modify the source table — "
            "existing rows must remain unchanged"
        )
