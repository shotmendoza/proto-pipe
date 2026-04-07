"""Tests for proto_pipe.cli.commands.refresh

Covers vp refresh source and vp refresh report behavioral guarantees.
"""

from datetime import datetime, timezone

import duckdb
import pytest
from click.testing import CliRunner
from unittest.mock import patch

from proto_pipe.cli.commands.refresh import refresh_source, refresh_report


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def refresh_db(tmp_path) -> str:
    """Pipeline DB seeded with source and validation state for refresh tests."""
    from proto_pipe.io.db import init_all_pipeline_tables

    db_path = str(tmp_path / "pipeline.db")
    now = datetime.now(timezone.utc)

    with duckdb.connect(db_path) as conn:
        init_all_pipeline_tables(conn)

        # Source table
        conn.execute(
            "CREATE TABLE sales (order_id VARCHAR, price DOUBLE, region VARCHAR)"
        )
        conn.execute("INSERT INTO sales VALUES ('ORD-001', 99.99, 'EMEA')")
        conn.execute("INSERT INTO sales VALUES ('ORD-002', 250.00, 'APAC')")
        conn.execute("INSERT INTO sales VALUES ('ORD-003', 15.50,  'EMEA')")

        # ORD-001: in source_block AND source_pass → stale flag, should be cleared
        conn.execute(
            """
            INSERT INTO source_block (id, table_name, check_name, pk_value, reason, flagged_at)
            VALUES ('id-001', 'sales', 'type_conflict', 'ORD-001', 'bad type', ?)
            """,
            [now],
        )
        conn.execute(
            """
            INSERT INTO source_pass (pk_value, table_name, row_hash, source_file, ingested_at)
            VALUES ('ORD-001', 'sales', 'hash001', 'sales_2026.csv', ?)
            """,
            [now],
        )

        # ORD-002: in source_block ONLY → still unresolved, must NOT be cleared
        conn.execute(
            """
            INSERT INTO source_block (id, table_name, check_name, pk_value, reason, flagged_at)
            VALUES ('id-002', 'sales', 'type_conflict', 'ORD-002', 'bad type', ?)
            """,
            [now],
        )

        # Validation state: ORD-001 in both validation_block and validation_pass
        conn.execute(
            """
            INSERT INTO validation_block
                (id, table_name, report_name, check_name, pk_value, reason, flagged_at)
            VALUES ('vb-001', 'sales', 'daily_sales_validation',
                    'range_check', 'ORD-001', 'out of range', ?)
            """,
            [now],
        )
        conn.execute(
            """
            INSERT INTO validation_pass
                (pk_value, table_name, report_name, row_hash, check_set_hash,
                 status, validated_at)
            VALUES ('ORD-001', 'sales', 'daily_sales_validation',
                    'hash001', 'cshash001', 'passed', ?)
            """,
            [now],
        )

        # ORD-002: in validation_block ONLY → still unresolved
        conn.execute(
            """
            INSERT INTO validation_block
                (id, table_name, report_name, check_name, pk_value, reason, flagged_at)
            VALUES ('vb-002', 'sales', 'daily_sales_validation',
                    'range_check', 'ORD-002', 'out of range', ?)
            """,
            [now],
        )

    return db_path


def _cfg(pipeline_db):
    return lambda key, override=None: {"pipeline_db": pipeline_db}.get(
        key, override or key
    )


# ---------------------------------------------------------------------------
# vp refresh source
# ---------------------------------------------------------------------------


class TestRefreshSource:
    """vp refresh source behavioral guarantees."""

    def test_clears_only_records_in_both_tables(self, refresh_db):
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=_cfg(refresh_db),
        ):
            result = runner.invoke(refresh_source, ["sales", "--yes"])

        assert result.exit_code == 0, result.output
        with duckdb.connect(refresh_db) as conn:
            remaining = conn.execute(
                "SELECT pk_value FROM source_block WHERE table_name = 'sales'"
            ).df()["pk_value"].tolist()

        assert "ORD-001" not in remaining, (
            "ORD-001 is in both source_block and source_pass — must be cleared"
        )
        assert "ORD-002" in remaining, (
            "ORD-002 is only in source_block — must not be cleared"
        )

    def test_unresolved_flags_are_not_touched(self, refresh_db):
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=_cfg(refresh_db),
        ):
            runner.invoke(refresh_source, ["sales", "--yes"])

        with duckdb.connect(refresh_db) as conn:
            count = conn.execute(
                "SELECT count(*) FROM source_block WHERE pk_value = 'ORD-002'"
            ).fetchone()[0]
        assert count == 1, "Unresolved flag for ORD-002 must remain in source_block"

    def test_source_pass_is_not_modified(self, refresh_db):
        with duckdb.connect(refresh_db) as conn:
            before = conn.execute(
                "SELECT count(*) FROM source_pass"
            ).fetchone()[0]

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=_cfg(refresh_db),
        ):
            runner.invoke(refresh_source, ["sales", "--yes"])

        with duckdb.connect(refresh_db) as conn:
            after = conn.execute(
                "SELECT count(*) FROM source_pass"
            ).fetchone()[0]

        assert before == after, "source_pass must not be modified by vp refresh source"

    def test_source_table_is_not_modified(self, refresh_db):
        with duckdb.connect(refresh_db) as conn:
            before = conn.execute("SELECT count(*) FROM sales").fetchone()[0]

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=_cfg(refresh_db),
        ):
            runner.invoke(refresh_source, ["sales", "--yes"])

        with duckdb.connect(refresh_db) as conn:
            after = conn.execute("SELECT count(*) FROM sales").fetchone()[0]

        assert before == after, "Source table must not be modified by vp refresh source"

    def test_no_name_refreshes_all_tables(self, tmp_path):
        """With no table_name, stale flags across all tables are cleared."""
        from proto_pipe.io.db import init_all_pipeline_tables

        db_path = str(tmp_path / "multi.db")
        now = datetime.now(timezone.utc)
        with duckdb.connect(db_path) as conn:
            init_all_pipeline_tables(conn)
            for table in ("sales", "inventory"):
                conn.execute(
                    f"CREATE TABLE {table} (pk VARCHAR)"
                )
                conn.execute(
                    """
                    INSERT INTO source_block
                        (id, table_name, check_name, pk_value, reason, flagged_at)
                    VALUES (?, ?, 'type_conflict', 'PK-001', 'bad type', ?)
                    """,
                    [f"id-{table}", table, now],
                )
                conn.execute(
                    """
                    INSERT INTO source_pass
                        (pk_value, table_name, row_hash, source_file, ingested_at)
                    VALUES ('PK-001', ?, 'hash001', 'file.csv', ?)
                    """,
                    [table, now],
                )

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=lambda key, override=None: db_path,
        ):
            result = runner.invoke(refresh_source, ["--yes"])

        assert result.exit_code == 0, result.output
        with duckdb.connect(db_path) as conn:
            count = conn.execute("SELECT count(*) FROM source_block").fetchone()[0]
        assert count == 0, "All stale flags across all tables must be cleared"

    def test_no_qualifying_records_shows_message(self, tmp_path):
        """When no records qualify, shows informational message and exits."""
        from proto_pipe.io.db import init_all_pipeline_tables

        db_path = str(tmp_path / "empty.db")
        with duckdb.connect(db_path) as conn:
            init_all_pipeline_tables(conn)

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=lambda key, override=None: db_path,
        ):
            result = runner.invoke(refresh_source, ["sales", "--yes"])

        assert result.exit_code == 0
        assert "nothing to clear" in result.output.lower()

    def test_yes_flag_skips_prompt(self, refresh_db):
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=_cfg(refresh_db),
        ):
            result = runner.invoke(refresh_source, ["sales", "--yes"])

        assert result.exit_code == 0
        assert "[ok]" in result.output

    def test_without_yes_prompts_before_deleting(self, refresh_db):
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=_cfg(refresh_db),
        ):
            result = runner.invoke(refresh_source, ["sales"], input="y\n")

        assert result.exit_code == 0
        assert "[ok]" in result.output

    def test_without_yes_cancelled_leaves_flags_intact(self, refresh_db):
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=_cfg(refresh_db),
        ):
            runner.invoke(refresh_source, ["sales"], input="n\n")

        with duckdb.connect(refresh_db) as conn:
            count = conn.execute(
                "SELECT count(*) FROM source_block WHERE pk_value = 'ORD-001'"
            ).fetchone()[0]
        assert count == 1, "Cancelling the prompt must leave flags intact"


# ---------------------------------------------------------------------------
# vp refresh report
# ---------------------------------------------------------------------------


class TestRefreshReport:
    """vp refresh report behavioral guarantees."""

    def test_clears_only_records_in_both_tables(self, refresh_db):
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=_cfg(refresh_db),
        ):
            result = runner.invoke(
                refresh_report, ["daily_sales_validation", "--yes"]
            )

        assert result.exit_code == 0, result.output
        with duckdb.connect(refresh_db) as conn:
            remaining = conn.execute(
                "SELECT pk_value FROM validation_block "
                "WHERE report_name = 'daily_sales_validation'"
            ).df()["pk_value"].tolist()

        assert "ORD-001" not in remaining, (
            "ORD-001 is in both validation_block and validation_pass — must be cleared"
        )
        assert "ORD-002" in remaining, (
            "ORD-002 is only in validation_block — must not be cleared"
        )

    def test_unresolved_flags_are_not_touched(self, refresh_db):
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=_cfg(refresh_db),
        ):
            runner.invoke(refresh_report, ["daily_sales_validation", "--yes"])

        with duckdb.connect(refresh_db) as conn:
            count = conn.execute(
                "SELECT count(*) FROM validation_block WHERE pk_value = 'ORD-002'"
            ).fetchone()[0]
        assert count == 1, "Unresolved flag for ORD-002 must remain in validation_block"

    def test_validation_pass_is_not_modified(self, refresh_db):
        with duckdb.connect(refresh_db) as conn:
            before = conn.execute(
                "SELECT count(*) FROM validation_pass"
            ).fetchone()[0]

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=_cfg(refresh_db),
        ):
            runner.invoke(refresh_report, ["daily_sales_validation", "--yes"])

        with duckdb.connect(refresh_db) as conn:
            after = conn.execute(
                "SELECT count(*) FROM validation_pass"
            ).fetchone()[0]

        assert before == after, (
            "validation_pass must not be modified by vp refresh report"
        )

    def test_no_name_refreshes_all_reports(self, tmp_path):
        """With no report_name, stale flags across all reports are cleared."""
        from proto_pipe.io.db import init_all_pipeline_tables

        db_path = str(tmp_path / "multi.db")
        now = datetime.now(timezone.utc)
        with duckdb.connect(db_path) as conn:
            init_all_pipeline_tables(conn)
            for report in ("report_a", "report_b"):
                conn.execute(
                    """
                    INSERT INTO validation_block
                        (id, table_name, report_name, check_name, pk_value,
                         reason, flagged_at)
                    VALUES (?, 'sales', ?, 'range_check', 'PK-001',
                            'out of range', ?)
                    """,
                    [f"id-{report}", report, now],
                )
                conn.execute(
                    """
                    INSERT INTO validation_pass
                        (pk_value, table_name, report_name, row_hash,
                         check_set_hash, status, validated_at)
                    VALUES ('PK-001', 'sales', ?, 'hash001', 'cshash', 'passed', ?)
                    """,
                    [report, now],
                )

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=lambda key, override=None: db_path,
        ):
            result = runner.invoke(refresh_report, ["--yes"])

        assert result.exit_code == 0, result.output
        with duckdb.connect(db_path) as conn:
            count = conn.execute(
                "SELECT count(*) FROM validation_block"
            ).fetchone()[0]
        assert count == 0, "All stale flags across all reports must be cleared"

    def test_no_qualifying_records_shows_message(self, tmp_path):
        from proto_pipe.io.db import init_all_pipeline_tables

        db_path = str(tmp_path / "empty.db")
        with duckdb.connect(db_path) as conn:
            init_all_pipeline_tables(conn)

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=lambda key, override=None: db_path,
        ):
            result = runner.invoke(
                refresh_report, ["daily_sales_validation", "--yes"]
            )

        assert result.exit_code == 0
        assert "nothing to clear" in result.output.lower()

    def test_yes_flag_skips_prompt(self, refresh_db):
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=_cfg(refresh_db),
        ):
            result = runner.invoke(
                refresh_report, ["daily_sales_validation", "--yes"]
            )

        assert result.exit_code == 0
        assert "[ok]" in result.output

    def test_without_yes_cancelled_leaves_flags_intact(self, refresh_db):
        runner = CliRunner()
        with patch(
            "proto_pipe.cli.commands.refresh.config_path_or_override",
            side_effect=_cfg(refresh_db),
        ):
            runner.invoke(
                refresh_report, ["daily_sales_validation"], input="n\n"
            )

        with duckdb.connect(refresh_db) as conn:
            count = conn.execute(
                "SELECT count(*) FROM validation_block WHERE pk_value = 'ORD-001'"
            ).fetchone()[0]
        assert count == 1, "Cancelling the prompt must leave flags intact"
