"""Integration tests for deliverable SQL generation.

Verifies that SQL produced by build_deliverable_sql is executable
against real DuckDB report tables and returns correct results.
"""

import duckdb
import pytest

from proto_pipe.cli.scaffold import (
    JoinSpec,
    DeliverableSQLSpec,
    build_deliverable_sql,
)


@pytest.fixture
def populated_db(tmp_path):
    """Create a DuckDB with two populated report tables."""
    db_path = str(tmp_path / "pipeline.db")
    conn = duckdb.connect(db_path)

    conn.execute("""
        CREATE TABLE premiums_report (
            policy_id VARCHAR,
            carrier VARCHAR,
            bound_premium DOUBLE,
            _ingested_at TIMESTAMP,
            _row_hash VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO premiums_report VALUES
            ('P001', 'Acme Re', 50000.0, '2025-01-01', 'h1'),
            ('P002', 'Beta Re', 75000.0, '2025-01-02', 'h2'),
            ('P003', 'Acme Re', 30000.0, '2025-01-03', 'h3')
    """)

    conn.execute("""
        CREATE TABLE claims_report (
            policy_id VARCHAR,
            carrier VARCHAR,
            claim_amount DOUBLE,
            _ingested_at TIMESTAMP,
            _row_hash VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO claims_report VALUES
            ('P001', 'Acme Re', 12000.0, '2025-01-01', 'h4'),
            ('P002', 'Beta Re', 5000.0, '2025-01-02', 'h5')
    """)

    yield conn
    conn.close()


class TestDeliverableSQLIntegration:
    """Generated SQL executes against real DuckDB and returns correct data."""

    def test_single_report_returns_selected_columns(self, populated_db):
        spec = DeliverableSQLSpec(
            deliverable_name="test",
            report_columns={"premiums_report": ["policy_id", "bound_premium"]},
        )
        sql = build_deliverable_sql(spec)
        result = populated_db.execute(sql).fetchdf()

        assert list(result.columns) == ["policy_id", "bound_premium"]
        assert len(result) == 3

    def test_single_report_subset_excludes_unselected(self, populated_db):
        spec = DeliverableSQLSpec(
            deliverable_name="test",
            report_columns={"premiums_report": ["policy_id", "carrier"]},
        )
        sql = build_deliverable_sql(spec)
        result = populated_db.execute(sql).fetchdf()

        assert "bound_premium" not in result.columns
        assert "policy_id" in result.columns
        assert "carrier" in result.columns

    def test_left_join_returns_all_left_rows(self, populated_db):
        spec = DeliverableSQLSpec(
            deliverable_name="combined",
            report_columns={
                "premiums_report": ["policy_id", "bound_premium"],
                "claims_report": ["claim_amount"],
            },
            join_specs=[
                JoinSpec("premiums_report", "claims_report",
                         "policy_id", "policy_id", "LEFT"),
            ],
        )
        sql = build_deliverable_sql(spec)
        result = populated_db.execute(sql).fetchdf()

        # LEFT JOIN: all 3 premium rows, P003 has NULL claim_amount
        assert len(result) == 3
        p003 = result[result["policy_id"] == "P003"]
        assert p003["claim_amount"].isna().all()

    def test_inner_join_returns_only_matching_rows(self, populated_db):
        spec = DeliverableSQLSpec(
            deliverable_name="combined",
            report_columns={
                "premiums_report": ["policy_id", "bound_premium"],
                "claims_report": ["claim_amount"],
            },
            join_specs=[
                JoinSpec("premiums_report", "claims_report",
                         "policy_id", "policy_id", "INNER"),
            ],
        )
        sql = build_deliverable_sql(spec)
        result = populated_db.execute(sql).fetchdf()

        # INNER JOIN: only P001 and P002 match
        assert len(result) == 2
        assert set(result["policy_id"]) == {"P001", "P002"}

    def test_order_by_sorts_results(self, populated_db):
        spec = DeliverableSQLSpec(
            deliverable_name="test",
            report_columns={"premiums_report": ["policy_id", "bound_premium"]},
            order_by="bound_premium",
        )
        sql = build_deliverable_sql(spec)
        result = populated_db.execute(sql).fetchdf()

        premiums = result["bound_premium"].tolist()
        assert premiums == sorted(premiums)

    def test_different_key_names_join_correctly(self, populated_db):
        """Tables with different key column names can still be joined."""
        # Create a table with a different key name
        populated_db.execute("""
            CREATE TABLE lookup_report (
                carrier_id VARCHAR,
                rating VARCHAR
            )
        """)
        populated_db.execute("""
            INSERT INTO lookup_report VALUES
                ('Acme Re', 'A+'),
                ('Beta Re', 'A')
        """)

        spec = DeliverableSQLSpec(
            deliverable_name="enriched",
            report_columns={
                "premiums_report": ["policy_id", "carrier"],
                "lookup_report": ["rating"],
            },
            join_specs=[
                JoinSpec("premiums_report", "lookup_report",
                         "carrier", "carrier_id", "LEFT"),
            ],
        )
        sql = build_deliverable_sql(spec)
        result = populated_db.execute(sql).fetchdf()

        assert len(result) == 3
        acme_rows = result[result["carrier"] == "Acme Re"]
        assert all(acme_rows["rating"] == "A+")
