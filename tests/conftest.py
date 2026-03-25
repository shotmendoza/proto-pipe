"""
Shared fixtures for the validation pipeline test suite.

All fixtures use tmp_path so every test gets a clean, isolated
filesystem and in-memory (or temp-file) DuckDB databases.
Nothing is written to the real project directories.
"""

from pathlib import Path

import duckdb
import pandas as pd
import pytest
import yaml


# ---------------------------------------------------------------------------
# Canonical fake data
# ---------------------------------------------------------------------------

SALES_ROWS = [
    {
        "order_id":    "ORD-001",
        "customer_id": "CUST-A",
        "price":       99.99,
        "quantity":    2,
        "region":      "EMEA",
        "order_date":  "2026-01-15",
        "updated_at":  "2026-01-15T10:00:00+00:00",
    },
    {
        "order_id":    "ORD-002",
        "customer_id": "CUST-B",
        "price":       250.00,
        "quantity":    1,
        "region":      "APAC",
        "order_date":  "2026-02-10",
        "updated_at":  "2026-02-10T09:00:00+00:00",
    },
    {
        "order_id":    "ORD-003",
        "customer_id": "CUST-C",
        "price":       15.50,
        "quantity":    5,
        "region":      "EMEA",
        "order_date":  "2026-03-01",
        "updated_at":  "2026-03-01T08:30:00+00:00",
    },
]

INVENTORY_ROWS = [
    {
        "sku":         "SKU-001",
        "description": "Widget A",
        "stock_level": 500,
        "updated_at":  "2026-01-20T12:00:00+00:00",
    },
    {
        "sku":         "SKU-002",
        "description": "Widget B",
        "stock_level": 0,
        "updated_at":  "2026-02-14T15:00:00+00:00",
    },
    {
        "sku":         "SKU-003",
        "description": "Widget C",
        "stock_level": 9999,
        "updated_at":  "2026-03-05T11:00:00+00:00",
    },
]


# ---------------------------------------------------------------------------
# DataFrame fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sales_df() -> pd.DataFrame:
    """Clean sales DataFrame — passes all built-in checks."""
    return pd.DataFrame(SALES_ROWS)


@pytest.fixture()
def inventory_df() -> pd.DataFrame:
    """Clean inventory DataFrame — passes all built-in checks."""
    return pd.DataFrame(INVENTORY_ROWS)


@pytest.fixture()
def sales_df_with_nulls(sales_df) -> pd.DataFrame:
    df = sales_df.copy()
    df.loc[0, "customer_id"] = None
    df.loc[2, "price"] = None
    return df


@pytest.fixture()
def sales_df_with_duplicates(sales_df) -> pd.DataFrame:
    return pd.concat([sales_df, sales_df.iloc[[0]]], ignore_index=True)


@pytest.fixture()
def sales_df_out_of_range(sales_df) -> pd.DataFrame:
    """One row with a negative price — violates range_check."""
    df = sales_df.copy()
    df.loc[1, "price"] = -5.00
    return df


# ---------------------------------------------------------------------------
# File fixtures (written to tmp_path)
# ---------------------------------------------------------------------------

@pytest.fixture()
def incoming_dir(tmp_path) -> Path:
    d = tmp_path / "incoming"
    d.mkdir()
    return d


@pytest.fixture()
def sales_csv(incoming_dir, sales_df) -> Path:
    path = incoming_dir / "sales_2026-03.csv"
    sales_df.to_csv(path, index=False)
    return path


@pytest.fixture()
def inventory_csv(incoming_dir, inventory_df) -> Path:
    path = incoming_dir / "inventory_2026-03.csv"
    inventory_df.to_csv(path, index=False)
    return path


@pytest.fixture()
def sales_csv_with_nulls(incoming_dir, sales_df_with_nulls) -> Path:
    path = incoming_dir / "sales_bad_nulls.csv"
    sales_df_with_nulls.to_csv(path, index=False)
    return path


@pytest.fixture()
def unmatched_csv(incoming_dir) -> Path:
    path = incoming_dir / "unknown_data_2026.csv"
    pd.DataFrame({"col": [1, 2]}).to_csv(path, index=False)
    return path


@pytest.fixture()
def empty_csv(incoming_dir) -> Path:
    path = incoming_dir / "sales_empty.csv"
    pd.DataFrame(columns=["order_id", "price", "updated_at"]).to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# DuckDB fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def pipeline_db(tmp_path) -> str:
    """Path to a fresh pipeline DuckDB file."""
    return str(tmp_path / "pipeline.db")


@pytest.fixture()
def watermark_db(tmp_path) -> str:
    """Path to a fresh watermark DuckDB file."""
    return str(tmp_path / "watermarks.db")


@pytest.fixture()
def db_conn(pipeline_db) -> duckdb.DuckDBPyConnection:
    """Open connection to the pipeline DB. Closed after the test."""
    conn = duckdb.connect(pipeline_db)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Source / report / deliverable config fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sources_config() -> dict:
    return {
        "sources": [
            {
                "name": "sales",
                "target_table": "sales",
                "timestamp_col": "updated_at",
                "patterns": ["sales_*.csv", "Sales_*.xlsx"],
            },
            {
                "name": "inventory",
                "target_table": "inventory",
                "timestamp_col": "updated_at",
                "patterns": ["inventory_*.csv"],
            },
        ]
    }


@pytest.fixture()
def reports_config() -> dict:
    return {
        "templates": {
            "standard_null_check": {"name": "null_check"},
            "price_range_check": {
                "name": "range_check",
                "params": {"col": "price", "min_val": 0, "max_val": 500},
            },
            "standard_schema": {
                "name": "schema_check",
                "params": {
                    "expected_cols": [
                        "order_id", "price", "quantity",
                        "customer_id", "region", "order_date", "updated_at",
                    ]
                },
            },
        },
        "reports": [
            {
                "name": "daily_sales_validation",
                "source": {
                    "type": "duckdb",
                    "path": "",          # filled in per-test via pipeline_db
                    "table": "sales",
                    "timestamp_col": "updated_at",
                },
                "options": {"parallel": False},
                "checks": [
                    {"template": "standard_null_check"},
                    {"template": "price_range_check"},
                    {"template": "standard_schema"},
                ],
            },
            {
                "name": "inventory_validation",
                "source": {
                    "type": "duckdb",
                    "path": "",
                    "table": "inventory",
                    "timestamp_col": "updated_at",
                },
                "options": {"parallel": False},
                "checks": [
                    {"template": "standard_null_check"},
                    {
                        "name": "range_check",
                        "params": {"col": "stock_level", "min_val": 0, "max_val": 10000},
                    },
                ],
            },
        ],
    }


@pytest.fixture()
def deliverables_config(tmp_path) -> dict:
    out_dir = str(tmp_path / "output")
    return {
        "deliverables": [
            {
                "name": "monthly_sales_pack",
                "format": "xlsx",
                "filename_template": "sales_{date}.xlsx",
                "output_dir": out_dir,
                "reports": [
                    {
                        "name": "daily_sales_validation",
                        "sheet": "Sales",
                        "filters": {
                            "date_filters": [
                                {"col": "order_date", "from": "2026-01-01", "to": "2026-03-31"}
                            ],
                            "field_filters": [
                                {"col": "region", "values": ["EMEA", "APAC"]}
                            ],
                        },
                    },
                    {
                        "name": "inventory_validation",
                        "sheet": "Inventory",
                        "filters": {
                            "date_filters": [
                                {"col": "updated_at", "from": "2026-01-01"}
                            ]
                        },
                    },
                ],
            },
            {
                "name": "ops_daily_drop",
                "format": "csv",
                "filename_template": "{report_name}_{date}.csv",
                "output_dir": out_dir,
                "reports": [
                    {
                        "name": "daily_sales_validation",
                        "filters": {
                            "date_filters": [
                                {"col": "order_date", "from": "2026-03-01"}
                            ]
                        },
                    }
                ],
            },
        ]
    }


# ---------------------------------------------------------------------------
# Config YAML file fixtures (written to tmp_path, used by YAML-loading tests)
# ---------------------------------------------------------------------------

@pytest.fixture()
def sources_config_path(tmp_path, sources_config) -> Path:
    path = tmp_path / "sources_config.yaml"
    path.write_text(yaml.dump(sources_config))
    return path


@pytest.fixture()
def reports_config_path(tmp_path, reports_config, pipeline_db) -> Path:
    # Patch source paths to point at the temp pipeline_db
    cfg = reports_config.copy()
    for r in cfg["reports"]:
        r["source"]["path"] = pipeline_db
    path = tmp_path / "reports_config.yaml"
    path.write_text(yaml.dump(cfg))
    return path


# ---------------------------------------------------------------------------
# Pre-populated DB fixture (ingest already run)
# ---------------------------------------------------------------------------

@pytest.fixture()
def populated_db(pipeline_db, sales_df, inventory_df) -> str:
    """Pipeline DB with sales and inventory tables already loaded."""
    conn = duckdb.connect(pipeline_db)
    conn.execute("CREATE TABLE sales AS SELECT * FROM sales_df")
    conn.execute("CREATE TABLE inventory AS SELECT * FROM inventory_df")
    conn.close()
    return pipeline_db


# ---------------------------------------------------------------------------
# Registry fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def check_registry():
    from src.registry.base import CheckRegistry
    return CheckRegistry()


@pytest.fixture()
def report_registry():
    from src.registry.base import ReportRegistry
    return ReportRegistry()


@pytest.fixture()
def watermark_store(watermark_db):
    from src.pipelines.watermark import WatermarkStore
    return WatermarkStore(watermark_db)
