
"""Unit tests for build_deliverable_sql in scaffold.py."""

import pytest

from proto_pipe.cli.scaffold import (
    JoinSpec,
    DeliverableSQLSpec,
    build_deliverable_sql,
)
from proto_pipe.cli.prompts import MacroApplication


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


class TestBuildDeliverableSQLWithViews:
    """View join SQL generation."""

    def test_single_report_plus_view_uses_aliases(self):
        """A single report + view produces aliased JOINs (not the unaliased path)."""
        spec = DeliverableSQLSpec(
            deliverable_name="with_view",
            report_columns={"premiums_report": ["policy_id", "bound_premium"]},
            view_columns={"carrier_summary": ["carrier", "total_premium"]},
            join_specs=[
                JoinSpec("premiums_report", "carrier_summary", "carrier", "carrier", "LEFT"),
            ],
        )
        sql = build_deliverable_sql(spec)

        assert "FROM premiums_report a" in sql
        assert "LEFT JOIN carrier_summary b" in sql
        assert "a.carrier = b.carrier" in sql
        assert "a.policy_id" in sql
        assert "a.bound_premium" in sql
        assert "b.total_premium" in sql
        # No unaliased columns
        assert "\n    policy_id" not in sql

    def test_single_report_no_views_unchanged(self):
        """When view_columns is empty, single report uses the unaliased path."""
        spec = DeliverableSQLSpec(
            deliverable_name="no_views",
            report_columns={"premiums_report": ["policy_id", "bound_premium"]},
            view_columns={},
        )
        sql = build_deliverable_sql(spec)

        assert "FROM premiums_report" in sql
        assert " a." not in sql
        assert "policy_id" in sql

    def test_two_reports_plus_view_all_aliased(self):
        """Two reports + one view → three aliases (a, b, c)."""
        spec = DeliverableSQLSpec(
            deliverable_name="triple",
            report_columns={
                "premiums_report": ["policy_id", "bound_premium"],
                "claims_report": ["policy_id", "claim_amount"],
            },
            view_columns={
                "carrier_summary": ["carrier", "total_premium"],
            },
            join_specs=[
                JoinSpec("premiums_report", "claims_report", "policy_id", "policy_id", "LEFT"),
                JoinSpec("premiums_report", "carrier_summary", "carrier", "carrier", "INNER"),
            ],
        )
        sql = build_deliverable_sql(spec)

        assert "FROM premiums_report a" in sql
        assert "LEFT JOIN claims_report b" in sql
        assert "INNER JOIN carrier_summary c" in sql
        assert "a.policy_id = b.policy_id" in sql
        assert "a.carrier = c.carrier" in sql
        assert "c.total_premium" in sql

    def test_view_columns_appear_after_report_columns(self):
        """View columns appear in SELECT after all report columns."""
        spec = DeliverableSQLSpec(
            deliverable_name="order_test",
            report_columns={"premiums_report": ["policy_id"]},
            view_columns={"carrier_summary": ["total_premium"]},
            join_specs=[
                JoinSpec("premiums_report", "carrier_summary", "carrier", "carrier", "LEFT"),
            ],
        )
        sql = build_deliverable_sql(spec)

        policy_pos = sql.index("a.policy_id")
        total_pos = sql.index("b.total_premium")
        assert policy_pos < total_pos

    def test_header_lists_views_separately(self):
        """SQL header has separate -- Reports: and -- Views: lines."""
        spec = DeliverableSQLSpec(
            deliverable_name="header_test",
            report_columns={"premiums_report": ["policy_id"]},
            view_columns={"carrier_summary": ["total_premium"]},
            join_specs=[
                JoinSpec("premiums_report", "carrier_summary", "carrier", "carrier", "LEFT"),
            ],
        )
        sql = build_deliverable_sql(spec)

        assert "-- Reports: premiums_report" in sql
        assert "-- Views: carrier_summary" in sql

    def test_no_views_header_omits_views_line(self):
        """When no views, the -- Views: line is absent."""
        spec = DeliverableSQLSpec(
            deliverable_name="no_view_header",
            report_columns={"premiums_report": ["policy_id"]},
        )
        sql = build_deliverable_sql(spec)

        assert "-- Reports: premiums_report" in sql
        assert "-- Views:" not in sql

    def test_view_with_different_join_keys(self):
        """View join where left and right keys have different names."""
        spec = DeliverableSQLSpec(
            deliverable_name="diff_keys",
            report_columns={"premiums_report": ["carrier_code", "bound_premium"]},
            view_columns={"carrier_summary": ["carrier_id", "total_premium"]},
            join_specs=[
                JoinSpec("premiums_report", "carrier_summary", "carrier_code", "carrier_id", "LEFT"),
            ],
        )
        sql = build_deliverable_sql(spec)

        assert "a.carrier_code = b.carrier_id" in sql

    def test_default_view_columns_is_empty(self):
        """DeliverableSQLSpec without view_columns defaults to empty dict."""
        spec = DeliverableSQLSpec(
            deliverable_name="default",
            report_columns={"r": ["col"]},
        )
        assert spec.view_columns == {}


class TestBuildDeliverableSQLMacros:
    """Macro application in SQL generation."""

    def test_macro_overwrites_replaces_column(self):
        """When overwrites is set, the original column is replaced."""
        spec = DeliverableSQLSpec(
            deliverable_name="with_macro",
            report_columns={"premiums_report": ["policy_id", "bound_premium", "carrier"]},
            macro_applications=[
                MacroApplication(
                    macro_name="apply_quota_share",
                    param_bindings={"premium": "bound_premium", "share": "0.5"},
                    output_column="adjusted_premium",
                    overwrites="bound_premium",
                ),
            ],
        )
        sql = build_deliverable_sql(spec)

        assert "apply_quota_share(bound_premium, 0.5) AS adjusted_premium" in sql
        # Original bound_premium should NOT appear as a standalone column
        select_part = sql.split("FROM")[0]
        lines = [l.strip() for l in select_part.split("\n") if "bound_premium" in l]
        for line in lines:
            assert "apply_quota_share" in line, (
                f"bound_premium should only appear inside macro expression, got: {line}"
            )

    def test_macro_no_overwrite_appends_column(self):
        """When overwrites is None, macro expression is appended."""
        spec = DeliverableSQLSpec(
            deliverable_name="with_macro",
            report_columns={"premiums_report": ["policy_id", "bound_premium"]},
            macro_applications=[
                MacroApplication(
                    macro_name="compute_tax",
                    param_bindings={"amount": "bound_premium"},
                    output_column="tax_amount",
                    overwrites=None,
                ),
            ],
        )
        sql = build_deliverable_sql(spec)

        assert "compute_tax(bound_premium) AS tax_amount" in sql
        assert "policy_id" in sql
        # Original bound_premium still present as its own column
        select_part = sql.split("FROM")[0]
        lines = [l.strip() for l in select_part.split("\n")
                 if l.strip() and "bound_premium" in l]
        # Should have two lines: one for the column, one for the macro
        has_standalone = any("compute_tax" not in l for l in lines)
        has_macro = any("compute_tax" in l for l in lines)
        assert has_standalone, "Original bound_premium column should still be present"
        assert has_macro, "Macro expression should be present"

    def test_multiple_macros_all_appear(self):
        """Multiple macro applications all appear in SELECT."""
        spec = DeliverableSQLSpec(
            deliverable_name="multi_macro",
            report_columns={"premiums_report": ["policy_id", "bound_premium", "carrier"]},
            macro_applications=[
                MacroApplication(
                    macro_name="apply_quota_share",
                    param_bindings={"premium": "bound_premium", "share": "0.5"},
                    output_column="adjusted_premium",
                    overwrites="bound_premium",
                ),
                MacroApplication(
                    macro_name="normalize_carrier",
                    param_bindings={"name": "carrier"},
                    output_column="carrier_norm",
                    overwrites=None,
                ),
            ],
        )
        sql = build_deliverable_sql(spec)

        assert "apply_quota_share(bound_premium, 0.5) AS adjusted_premium" in sql
        assert "normalize_carrier(carrier) AS carrier_norm" in sql

    def test_no_macros_sql_unchanged(self):
        """Empty macro_applications list produces identical SQL to pre-macro behavior."""
        spec = DeliverableSQLSpec(
            deliverable_name="no_macros",
            report_columns={"premiums_report": ["policy_id", "bound_premium"]},
        )
        sql = build_deliverable_sql(spec)

        assert "policy_id" in sql
        assert "bound_premium" in sql
        assert "-- Macros:" not in sql

    def test_macro_header_lists_macros(self):
        """SQL header includes -- Macros: line when macros applied."""
        spec = DeliverableSQLSpec(
            deliverable_name="header_test",
            report_columns={"premiums_report": ["policy_id"]},
            macro_applications=[
                MacroApplication(
                    macro_name="my_macro",
                    param_bindings={"x": "policy_id"},
                    output_column="result",
                    overwrites=None,
                ),
            ],
        )
        sql = build_deliverable_sql(spec)
        assert "-- Macros: my_macro" in sql

    def test_macro_with_qualified_args_in_multi_table(self):
        """Macro args use table.column format resolved to aliases."""
        spec = DeliverableSQLSpec(
            deliverable_name="multi",
            report_columns={
                "premiums_report": ["policy_id", "bound_premium"],
                "claims_report": ["policy_id", "claim_amount"],
            },
            join_specs=[
                JoinSpec("premiums_report", "claims_report", "policy_id", "policy_id", "LEFT"),
            ],
            macro_applications=[
                MacroApplication(
                    macro_name="net_amount",
                    param_bindings={
                        "premium": "premiums_report.bound_premium",
                        "claim": "claims_report.claim_amount",
                    },
                    output_column="net",
                    overwrites=None,
                ),
            ],
        )
        sql = build_deliverable_sql(spec)

        assert "net_amount(a.bound_premium, b.claim_amount) AS net" in sql

    def test_macro_overwrites_in_multi_table(self):
        """Overwrite works with aliased columns in multi-table SQL."""
        spec = DeliverableSQLSpec(
            deliverable_name="overwrite_multi",
            report_columns={
                "premiums_report": ["policy_id", "bound_premium"],
                "claims_report": ["policy_id", "claim_amount"],
            },
            join_specs=[
                JoinSpec("premiums_report", "claims_report", "policy_id", "policy_id", "LEFT"),
            ],
            macro_applications=[
                MacroApplication(
                    macro_name="adjust",
                    param_bindings={"val": "bound_premium"},
                    output_column="bound_premium",
                    overwrites="bound_premium",
                ),
            ],
        )
        sql = build_deliverable_sql(spec)

        assert "adjust(a.bound_premium) AS bound_premium" in sql
        # Only one reference to bound_premium in SELECT
        select_part = sql.split("FROM")[0]
        lines = [l.strip() for l in select_part.split("\n")
                 if l.strip() and "bound_premium" in l]
        assert len(lines) == 1, f"Expected 1 bound_premium line, got: {lines}"

    def test_macro_literal_arg_not_aliased(self):
        """Literal values in param_bindings are not alias-qualified."""
        spec = DeliverableSQLSpec(
            deliverable_name="literal",
            report_columns={"premiums_report": ["policy_id", "bound_premium"]},
            macro_applications=[
                MacroApplication(
                    macro_name="scale",
                    param_bindings={"val": "bound_premium", "factor": "100"},
                    output_column="scaled",
                    overwrites=None,
                ),
            ],
        )
        sql = build_deliverable_sql(spec)
        assert "scale(bound_premium, 100) AS scaled" in sql

    def test_default_macro_applications_is_empty_list(self):
        """DeliverableSQLSpec without macro_applications defaults to empty list."""
        spec = DeliverableSQLSpec(
            deliverable_name="default",
            report_columns={"r": ["col"]},
        )
        assert spec.macro_applications == []

    def test_macro_with_decimal_literal(self):
        """Decimal literal like '0.5' is not mistaken for table.column."""
        spec = DeliverableSQLSpec(
            deliverable_name="decimal",
            report_columns={"premiums_report": ["policy_id", "bound_premium"]},
            macro_applications=[
                MacroApplication(
                    macro_name="apply_share",
                    param_bindings={"premium": "bound_premium", "share": "0.5"},
                    output_column="shared",
                    overwrites=None,
                ),
            ],
        )
        sql = build_deliverable_sql(spec)
        assert "apply_share(bound_premium, 0.5) AS shared" in sql
