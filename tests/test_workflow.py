"""End-to-end workflow integration tests.

These tests exercise the full pipeline as an analyst would use it:
  ingest → validate → deliver

Each test class represents a distinct analyst scenario.
No mocking — real DuckDB, real files, real output.
"""
import uuid
from pathlib import Path

import duckdb
import pandas as pd

from proto_pipe.io.ingest import ingest_directory
from proto_pipe.io.registry import register_from_config
from proto_pipe.pipelines.watermark import WatermarkStore
from proto_pipe.registry.base import CheckRegistry, ReportRegistry
from proto_pipe.reports.query import query_table
from proto_pipe.reports.runner import run_deliverable, run_all_reports


# ---------------------------------------------------------------------------
# Scenario 1 — Clean morning run (all passes, deliverable produced)
# ---------------------------------------------------------------------------

class TestCleanMorningRun:
    def test_full_pipeline_produces_xlsx(
        self,
        tmp_path,
        incoming_dir,
        sales_csv,
        inventory_csv,
        pipeline_db,
        watermark_db,
        sources_config,
        reports_config,
        deliverables_config,
    ):
        # Patch report source paths
        for r in reports_config["reports"]:
            r["source"]["path"] = pipeline_db

        # 1. Ingest
        summary = ingest_directory(
            str(incoming_dir), sources_config["sources"], pipeline_db
        )
        assert all(v["status"] == "ok" for v in summary.values())

        # 2. Validate
        cr = CheckRegistry()
        rr = ReportRegistry()
        ws = WatermarkStore(watermark_db)
        register_from_config(reports_config, cr, rr)
        results = run_all_reports(rr, cr, ws, parallel_reports=False)

        for r in results:
            assert r["status"] == "completed"

        # 3. Deliver
        conn = duckdb.connect(pipeline_db)
        deliverable = deliverables_config["deliverables"][0]  # monthly_sales_pack
        out_dir = tmp_path / "output"
        dfs = {
            "daily_sales_validation": query_table(conn, "sales"),
            "inventory_validation": query_table(conn, "inventory"),
        }
        conn.close()

        run_deliverable(deliverable, dfs, str(out_dir), pipeline_db, run_date="2026-03-26")
        files = list(Path(deliverable["output_dir"]).glob("*.xlsx"))
        assert len(files) == 1

    def test_report_runs_logged(
        self,
        tmp_path,
        incoming_dir,
        sales_csv,
        inventory_csv,
        pipeline_db,
        watermark_db,
        sources_config,
        reports_config,
        deliverables_config,
    ):
        for r in reports_config["reports"]:
            r["source"]["path"] = pipeline_db

        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)

        cr = CheckRegistry()
        rr = ReportRegistry()
        ws = WatermarkStore(watermark_db)
        register_from_config(reports_config, cr, rr)
        run_all_reports(rr, cr, ws, parallel_reports=False)

        conn = duckdb.connect(pipeline_db)
        dfs = {
            "daily_sales_validation": query_table(conn, "sales"),
            "inventory_validation": query_table(conn, "inventory"),
        }
        conn.close()

        deliverable = deliverables_config["deliverables"][0]
        run_deliverable(deliverable, dfs, str(tmp_path), pipeline_db, run_date="2026-03-26")

        conn = duckdb.connect(pipeline_db)
        count = conn.execute("SELECT count(*) FROM report_runs").fetchone()[0]
        conn.close()
        assert count >= 2


# ---------------------------------------------------------------------------
# Scenario 2 — Second run only processes new rows (watermark filtering)
# ---------------------------------------------------------------------------

class TestWatermarkFiltering:
    def test_second_ingest_skips_already_validated_rows(
            self,
            tmp_path,
            incoming_dir,
            sales_csv,
            inventory_csv,
            pipeline_db,
            watermark_db,
            sources_config,
            reports_config,
    ):
        for r in reports_config["reports"]:
            r["source"]["path"] = pipeline_db

        # First run
        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)
        cr = CheckRegistry()
        rr = ReportRegistry()
        ws = WatermarkStore(watermark_db)
        register_from_config(reports_config, cr, rr)
        run_all_reports(rr, cr, ws, parallel_reports=False)

        # Second run — same data, watermark should skip all rows
        results = run_all_reports(rr, cr, ws, parallel_reports=False)
        sales_result = next(r for r in results if r["report"] == "daily_sales_validation")
        assert sales_result["status"] == "skipped"


# ---------------------------------------------------------------------------
# Scenario 3 — Bad file arrives, good file still ingested
# ---------------------------------------------------------------------------

class TestPartialIngestFailure:
    def test_good_file_ingested_despite_bad_file(
        self,
        incoming_dir,
        sales_csv,
        empty_csv,
        pipeline_db,
        sources_config,
    ):
        summary = ingest_directory(
            str(incoming_dir), sources_config["sources"], pipeline_db
        )
        assert summary["sales_2026-03.csv"]["status"] == "ok"
        assert summary["sales_empty.csv"]["status"] == "failed"

        conn = duckdb.connect(pipeline_db)
        count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        conn.close()
        assert count == 3


# ---------------------------------------------------------------------------
# Scenario 4 — Analyst appends a corrected file mid-day
# ---------------------------------------------------------------------------

class TestMidDayAppend:
    def test_append_adds_new_rows(
        self,
        incoming_dir,
        sales_csv,
        pipeline_db,
        sources_config,
        sales_df,
    ):
        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)

        # New file arrives with one extra row
        new_row = {
            "order_id":    "ORD-004",
            "customer_id": "CUST-D",
            "price":       75.00,
            "quantity":    3,
            "region":      "EMEA",
            "order_date":  "2026-03-26",
            "updated_at":  "2026-03-26T14:00:00+00:00",
        }
        new_df = pd.concat(
            [sales_df, pd.DataFrame([new_row])], ignore_index=True
        )
        new_path = incoming_dir / "sales_2026-03-corrected.csv"
        new_df.to_csv(new_path, index=False)

        ingest_directory(
            str(incoming_dir), sources_config["sources"], pipeline_db, mode="append"
        )

        conn = duckdb.connect(pipeline_db)
        count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        conn.close()

        # The first `ingest` loads sales_2026-03.csv (3 rows).
        # The second `ingest` skips it (already ingested) and loads sales_2026-03-corrected.csv (4 rows)
        assert count == 7

    def test_replace_rebuilds_table(
        self,
        incoming_dir,
        sales_csv,
        pipeline_db,
        sources_config,
        sales_df,
    ):
        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)

        corrected_path = incoming_dir / "sales_2026-04-corrected.csv"
        sales_df.iloc[:2].to_csv(corrected_path, index=False)

        ingest_directory(
            str(incoming_dir), sources_config["sources"], pipeline_db, mode="replace"
        )

        conn = duckdb.connect(pipeline_db)
        # replace mode: last file wins — 2 rows from the corrected file
        count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
        conn.close()
        # Each replace run resets; the corrected file has 2 rows
        assert count == 2


# ---------------------------------------------------------------------------
# Scenario 5 — Ad-hoc pull with date override
# ---------------------------------------------------------------------------

class TestAdHocDateOverride:
    def test_cli_date_override_narrows_results(
        self,
        incoming_dir,
        sales_csv,
        pipeline_db,
        sources_config,
    ):
        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)

        conn = duckdb.connect(pipeline_db)
        df = query_table(
            conn,
            "sales",
            filters={"date_filters": [{"col": "order_date", "from": "2026-01-01"}]},
            cli_overrides={
                "date_filters": [
                    {"col": "order_date", "from": "2026-03-01", "to": "2026-03-31"}
                ]
            },
        )
        conn.close()

        assert len(df) == 1
        assert df.iloc[0]["order_id"] == "ORD-003"


# ---------------------------------------------------------------------------
# Scenario 6 — Custom check registered and runs in pipeline
# ---------------------------------------------------------------------------

class TestCustomCheckInPipeline:
    def test_custom_check_runs_via_pipeline(
        self,
        tmp_path,
        incoming_dir,
        sales_csv,
        inventory_csv,
        pipeline_db,
        watermark_db,
        sources_config,
        reports_config,
    ):
        from proto_pipe import custom_check
        from proto_pipe.checks.built_in import BUILT_IN_CHECKS

        @custom_check("quantity_positive")
        def check_quantity(context):
            df = context["df"]
            bad = df[df["quantity"] <= 0]
            return {"violations": len(bad)}

        # Register custom check into BUILT_IN_CHECKS so loader can find it
        BUILT_IN_CHECKS["quantity_positive"] = check_quantity

        # Add it to the sales report config
        for r in reports_config["reports"]:
            r["source"]["path"] = pipeline_db
            if r["name"] == "daily_sales_validation":
                r["checks"].append({"name": "quantity_positive"})

        ingest_directory(str(incoming_dir), sources_config["sources"], pipeline_db)

        cr = CheckRegistry()
        rr = ReportRegistry()
        ws = WatermarkStore(watermark_db)
        register_from_config(reports_config, cr, rr)
        results = run_all_reports(rr, cr, ws, parallel_reports=False)

        sales_result = next(r for r in results if r["report"] == "daily_sales_validation")
        expected_key = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"quantity_positive:{sorted({}.items())}"))
        assert expected_key in sales_result["results"]
        assert sales_result["results"][expected_key]["result"]["violations"] == 0
        assert sales_result["status"] == "completed"
