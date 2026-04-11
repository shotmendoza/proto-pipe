"""Behavioral guarantee tests for macro integration in vp new deliverable.

These tests verify SQL output guarantees at the build_deliverable_sql
level. Full CLI-level behavioral tests (mocking prompter.run() and
asserting config + SQL file writes) should be added to
test_vp_new_deliverable_guarantees.py when it exists.

Each test maps to one user-observable guarantee.
"""

import pytest

from proto_pipe.cli.scaffold import (
    JoinSpec,
    DeliverableSQLSpec,
    build_deliverable_sql,
)
from proto_pipe.cli.prompts import MacroApplication


class TestDeliverableMacroGuarantees:
    """Each test verifies one macro-related guarantee."""

    # ----- Guarantee: Macro selected → SQL includes macro call -----

    def test_macro_selected_produces_sql_with_macro_call(self):
        """When a macro is applied, the generated SQL includes the macro
        function call with correct arguments and output alias."""
        spec = DeliverableSQLSpec(
            deliverable_name="carrier_a",
            report_columns={"premiums_report": ["policy_id", "bound_premium"]},
            macro_applications=[
                MacroApplication(
                    macro_name="apply_quota_share",
                    param_bindings={"premium": "bound_premium", "share": "0.5"},
                    output_column="adjusted_premium",
                    overwrites=None,
                ),
            ],
        )
        sql = build_deliverable_sql(spec)

        assert "apply_quota_share(bound_premium, 0.5) AS adjusted_premium" in sql
        assert "-- Macros: apply_quota_share" in sql
        # Original columns still present
        assert "policy_id" in sql

    # ----- Guarantee: No macros → wizard skips, SQL unchanged -----

    def test_no_macros_available_produces_standard_sql(self):
        """When no macros are configured (empty list), SQL is identical
        to the pre-macro behavior with no -- Macros: header."""
        spec = DeliverableSQLSpec(
            deliverable_name="no_macros",
            report_columns={"premiums_report": ["policy_id", "bound_premium"]},
            macro_applications=[],
        )
        sql = build_deliverable_sql(spec)

        assert "-- Macros:" not in sql
        assert "policy_id" in sql
        assert "bound_premium" in sql
        assert sql.strip().endswith(";")

    # ----- Guarantee: Overwrite column → not duplicated -----

    def test_overwrite_column_not_duplicated_in_final_sql(self):
        """When a macro overwrites a column, the original column does
        not appear separately — only the macro expression with the
        same output alias."""
        spec = DeliverableSQLSpec(
            deliverable_name="overwrite",
            report_columns={
                "premiums_report": ["policy_id", "bound_premium", "carrier"],
            },
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

        assert "adjust(bound_premium) AS bound_premium" in sql
        # Only one line in SELECT references bound_premium
        select_part = sql.split("FROM")[0]
        lines = [
            l.strip() for l in select_part.split("\n")
            if l.strip() and "bound_premium" in l
        ]
        assert len(lines) == 1, (
            f"bound_premium should appear in exactly 1 SELECT line, got: {lines}"
        )

    # ----- Guarantee: Ambiguous column → qualified binding resolves -----

    def test_ambiguous_column_resolved_via_qualified_binding(self):
        """When a column exists in multiple tables, the wizard stores
        'table.column' in param_bindings. build_deliverable_sql
        resolves this to the correct alias."""
        spec = DeliverableSQLSpec(
            deliverable_name="ambiguous",
            report_columns={
                "premiums_report": ["policy_id", "amount"],
                "claims_report": ["policy_id", "amount"],
            },
            join_specs=[
                JoinSpec("premiums_report", "claims_report",
                         "policy_id", "policy_id", "LEFT"),
            ],
            macro_applications=[
                MacroApplication(
                    macro_name="double",
                    param_bindings={"val": "claims_report.amount"},
                    output_column="doubled_claim",
                    overwrites=None,
                ),
            ],
        )
        sql = build_deliverable_sql(spec)

        # claims_report gets alias 'b'
        assert "double(b.amount) AS doubled_claim" in sql
