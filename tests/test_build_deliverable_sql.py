
"""Unit tests for build_deliverable_sql in scaffold.py."""

import pytest

from proto_pipe.cli.scaffold import (
    JoinSpec,
    DeliverableSQLSpec,
    build_deliverable_sql,
)


class TestBuildDeliverableSQLSingleReport:
    """Single-report SQL generation — no aliases, no JOINs."""

    def test_all_columns_produces_select_with_all_columns(self):
        spec = DeliverableSQLSpec(
            deliverable_name="carrier_a",
            report_columns={"premiums_report": ["policy_id", "carrier", "bound_premium"]},
        )
        sql = build_deliverable_sql(spec)

        assert "SELECT" in sql
        assert "policy_id" in sql
        assert "carrier" in sql
        assert "bound_premium" in sql
        assert "FROM premiums_report" in sql
        # No aliases for single report
        assert " a." not in sql

    def test_subset_of_columns_excludes_unselected(self):
        spec = DeliverableSQLSpec(
            deliverable_name="carrier_a",
            report_columns={"premiums_report": ["policy_id", "carrier"]},
        )
        sql = build_deliverable_sql(spec)

        assert "policy_id" in sql
        assert "carrier" in sql
        assert "bound_premium" not in sql

    def test_column_ordering_matches_selection_order(self):
        columns = ["carrier", "policy_id", "bound_premium"]
        spec = DeliverableSQLSpec(
            deliverable_name="test",
            report_columns={"premiums_report": columns},
        )
        sql = build_deliverable_sql(spec)

        # Find positions in generated SQL
        carrier_pos = sql.index("carrier")
        policy_pos = sql.index("policy_id")
        premium_pos = sql.index("bound_premium")

        assert carrier_pos < policy_pos < premium_pos

    def test_order_by_included_when_specified(self):
        spec = DeliverableSQLSpec(
            deliverable_name="test",
            report_columns={"premiums_report": ["policy_id", "carrier"]},
            order_by="policy_id",
        )
        sql = build_deliverable_sql(spec)
        assert "ORDER BY policy_id" in sql

    def test_no_order_by_when_none(self):
        spec = DeliverableSQLSpec(
            deliverable_name="test",
            report_columns={"premiums_report": ["policy_id"]},
            order_by=None,
        )
        sql = build_deliverable_sql(spec)
        assert "ORDER BY" not in sql


class TestBuildDeliverableSQLMultiReport:
    """Multi-report SQL generation — aliases and JOINs."""

    def test_two_reports_with_join_key(self):
        spec = DeliverableSQLSpec(
            deliverable_name="combined",
            report_columns={
                "premiums_report": ["policy_id", "bound_premium"],
                "claims_report": ["policy_id", "claim_amount"],
            },
            join_specs=[
                JoinSpec(
                    left_table="premiums_report",
                    right_table="claims_report",
                    left_key="policy_id",
                    right_key="policy_id",
                    join_type="LEFT",
                ),
            ],
        )
        sql = build_deliverable_sql(spec)

        assert "FROM premiums_report a" in sql
        assert "LEFT JOIN claims_report b" in sql
        assert "a.policy_id = b.policy_id" in sql
        assert "a.bound_premium" in sql
        assert "b.claim_amount" in sql

    def test_different_key_names(self):
        spec = DeliverableSQLSpec(
            deliverable_name="combined",
            report_columns={
                "premiums_report": ["pk", "bound_premium"],
                "claims_report": ["foreign_pk", "claim_amount"],
            },
            join_specs=[
                JoinSpec(
                    left_table="premiums_report",
                    right_table="claims_report",
                    left_key="pk",
                    right_key="foreign_pk",
                    join_type="INNER",
                ),
            ],
        )
        sql = build_deliverable_sql(spec)

        assert "INNER JOIN claims_report b" in sql
        assert "a.pk = b.foreign_pk" in sql

    def test_full_outer_join(self):
        spec = DeliverableSQLSpec(
            deliverable_name="combined",
            report_columns={
                "report_a": ["col1"],
                "report_b": ["col2"],
            },
            join_specs=[
                JoinSpec(
                    left_table="report_a",
                    right_table="report_b",
                    left_key="id",
                    right_key="id",
                    join_type="FULL OUTER",
                ),
            ],
        )
        sql = build_deliverable_sql(spec)
        assert "FULL OUTER JOIN report_b b" in sql

    def test_three_reports_join_back_to_base(self):
        spec = DeliverableSQLSpec(
            deliverable_name="triple",
            report_columns={
                "base": ["id", "val_a"],
                "second": ["id", "val_b"],
                "third": ["id", "val_c"],
            },
            join_specs=[
                JoinSpec("base", "second", "id", "id", "LEFT"),
                JoinSpec("base", "third", "id", "id", "LEFT"),
            ],
        )
        sql = build_deliverable_sql(spec)

        assert "FROM base a" in sql
        assert "LEFT JOIN second b" in sql
        assert "LEFT JOIN third c" in sql
        assert "a.id = b.id" in sql
        assert "a.id = c.id" in sql

    def test_order_by_qualified_with_base_alias(self):
        spec = DeliverableSQLSpec(
            deliverable_name="combined",
            report_columns={
                "report_a": ["id", "col1"],
                "report_b": ["id", "col2"],
            },
            join_specs=[
                JoinSpec("report_a", "report_b", "id", "id", "LEFT"),
            ],
            order_by="id",
        )
        sql = build_deliverable_sql(spec)
        assert "ORDER BY a.id" in sql

    def test_header_lists_all_reports(self):
        spec = DeliverableSQLSpec(
            deliverable_name="combined",
            report_columns={
                "report_a": ["col1"],
                "report_b": ["col2"],
            },
            join_specs=[
                JoinSpec("report_a", "report_b", "id", "id", "LEFT"),
            ],
        )
        sql = build_deliverable_sql(spec)
        assert "-- Reports: report_a, report_b" in sql

    def test_sql_ends_with_semicolon(self):
        spec = DeliverableSQLSpec(
            deliverable_name="test",
            report_columns={"r": ["col"]},
        )
        sql = build_deliverable_sql(spec)
        assert sql.strip().endswith(";")


class TestBuildDeliverableSQLEdgeCases:
    """Edge cases."""

    def test_empty_report_columns_produces_placeholder(self):
        spec = DeliverableSQLSpec(
            deliverable_name="empty",
            report_columns={},
        )
        sql = build_deliverable_sql(spec)
        assert "SELECT *" in sql
        assert "<table>" in sql