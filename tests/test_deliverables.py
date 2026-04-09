"""
Tests for validation_pipeline.deliverables

Covers:
- resolve_filename (placeholder substitution)
- write_csv / write_xlsx_sheet (file created, readable, correct content)
- produce_deliverable: xlsx format (one file, multiple sheets)
- produce_deliverable: csv format (one file per report)
- report_runs table is populated after produce_deliverable
"""

from pathlib import Path

import duckdb
import pandas as pd

from proto_pipe.io.registry import resolve_filename, write_csv, write_xlsx_sheet
from proto_pipe.reports.deliverable import run_deliverable


# ---------------------------------------------------------------------------
# resolve_filename
# ---------------------------------------------------------------------------

class TestResolveFilename:
    def test_date_placeholder(self):
        assert resolve_filename("sales_{date}.xlsx", "rpt", "2026-03-26") == "sales_2026-03-26.xlsx"

    def test_report_name_placeholder(self):
        assert resolve_filename("{report_name}_{date}.csv", "daily_sales", "2026-03-26") == \
               "daily_sales_2026-03-26.csv"

    def test_no_placeholders(self):
        assert resolve_filename("output.csv", "rpt", "2026-03-26") == "output.csv"

    def test_both_placeholders(self):
        result = resolve_filename("{report_name}_{date}.csv", "sales", "2026-03-26")
        assert result == "sales_2026-03-26.csv"


# ---------------------------------------------------------------------------
# write_csv
# ---------------------------------------------------------------------------

class TestWriteCsv:
    def test_file_is_created(self, tmp_path, sales_df):
        out = tmp_path / "out.csv"
        write_csv(sales_df, out)
        assert out.exists()

    def test_content_is_readable(self, tmp_path, sales_df):
        out = tmp_path / "out.csv"
        write_csv(sales_df, out)
        df = pd.read_csv(out)
        assert len(df) == len(sales_df)
        assert list(df.columns) == list(sales_df.columns)

    def test_creates_parent_dirs(self, tmp_path, sales_df):
        out = tmp_path / "nested" / "deep" / "out.csv"
        write_csv(sales_df, out)
        assert out.exists()


# ---------------------------------------------------------------------------
# write_xlsx_sheet
# ---------------------------------------------------------------------------

class TestWriteXlsxSheet:
    def test_file_is_created(self, tmp_path, sales_df, inventory_df):
        out = tmp_path / "out.xlsx"
        write_xlsx_sheet({"Sales": sales_df, "Inventory": inventory_df}, out)
        assert out.exists()

    def test_sheets_are_present(self, tmp_path, sales_df, inventory_df):
        out = tmp_path / "out.xlsx"
        write_xlsx_sheet({"Sales": sales_df, "Inventory": inventory_df}, out)
        xl = pd.ExcelFile(out)
        assert "Sales" in xl.sheet_names
        assert "Inventory" in xl.sheet_names

    def test_sheet_content_correct(self, tmp_path, sales_df):
        out = tmp_path / "out.xlsx"
        write_xlsx_sheet({"Sales": sales_df}, out)
        df = pd.read_excel(out, sheet_name="Sales")
        assert len(df) == len(sales_df)


# ---------------------------------------------------------------------------
# produce_deliverable — xlsx
# ---------------------------------------------------------------------------

class TestProduceDeliverableXlsx:
    def test_xlsx_file_created(
        self, tmp_path, pipeline_db, sales_df, inventory_df, deliverables_config
    ):
        deliverable = deliverables_config["deliverables"][0]  # monthly_sales_pack
        dfs = {
            "daily_sales_validation": sales_df,
            "inventory_validation": inventory_df,
        }
        run_deliverable(deliverable, dfs, str(tmp_path), pipeline_db, run_date="2026-03-26")
        out_dir = Path(deliverable["output_dir"])
        files = list(out_dir.glob("*.xlsx"))
        assert len(files) == 1

    def test_xlsx_has_correct_sheets(
        self, tmp_path, pipeline_db, sales_df, inventory_df, deliverables_config
    ):
        deliverable = deliverables_config["deliverables"][0]
        dfs = {
            "daily_sales_validation": sales_df,
            "inventory_validation": inventory_df,
        }
        run_deliverable(deliverable, dfs, str(tmp_path), pipeline_db, run_date="2026-03-26")
        out_dir = Path(deliverable["output_dir"])
        xl_path = next(out_dir.glob("*.xlsx"))
        xl = pd.ExcelFile(xl_path)
        assert "Sales" in xl.sheet_names
        assert "Inventory" in xl.sheet_names

    def test_xlsx_filename_uses_run_date(
        self, tmp_path, pipeline_db, sales_df, inventory_df, deliverables_config
    ):
        deliverable = deliverables_config["deliverables"][0]
        dfs = {"daily_sales_validation": sales_df, "inventory_validation": inventory_df}
        run_deliverable(deliverable, dfs, str(tmp_path), pipeline_db, run_date="2026-03-26")
        out_dir = Path(deliverable["output_dir"])
        files = list(out_dir.glob("*.xlsx"))
        assert "2026-03-26" in files[0].name


# ---------------------------------------------------------------------------
# produce_deliverable — csv
# ---------------------------------------------------------------------------

class TestProduceDeliverableCsv:
    def test_csv_file_created(
        self, tmp_path, pipeline_db, sales_df, deliverables_config
    ):
        deliverable = deliverables_config["deliverables"][1]  # ops_daily_drop
        dfs = {"daily_sales_validation": sales_df}
        run_deliverable(deliverable, dfs, str(tmp_path), pipeline_db, run_date="2026-03-26")
        out_dir = Path(deliverable["output_dir"])
        files = list(out_dir.glob("*.csv"))
        assert len(files) == 1

    def test_csv_content_correct(
        self, tmp_path, pipeline_db, sales_df, deliverables_config
    ):
        deliverable = deliverables_config["deliverables"][1]
        dfs = {"daily_sales_validation": sales_df}
        run_deliverable(deliverable, dfs, str(tmp_path), pipeline_db, run_date="2026-03-26")
        out_dir = Path(deliverable["output_dir"])
        csv_path = next(out_dir.glob("*.csv"))
        df = pd.read_csv(csv_path)
        assert len(df) == len(sales_df)


# ---------------------------------------------------------------------------
# report_runs table
# ---------------------------------------------------------------------------

class TestReportRunsLog:
    def test_run_is_logged(
        self, tmp_path, pipeline_db, sales_df, inventory_df, deliverables_config
    ):
        deliverable = deliverables_config["deliverables"][0]
        dfs = {
            "daily_sales_validation": sales_df,
            "inventory_validation": inventory_df,
        }
        run_deliverable(deliverable, dfs, str(tmp_path), pipeline_db, run_date="2026-03-26")

        conn = duckdb.connect(pipeline_db)
        rows = conn.execute(
            "SELECT report_name, row_count, format FROM report_runs"
        ).fetchall()
        conn.close()

        report_names = {r[0] for r in rows}
        assert "daily_sales_validation" in report_names
        assert "inventory_validation" in report_names

    def test_row_counts_are_correct(
        self, tmp_path, pipeline_db, sales_df, inventory_df, deliverables_config
    ):
        deliverable = deliverables_config["deliverables"][0]
        dfs = {
            "daily_sales_validation": sales_df,
            "inventory_validation": inventory_df,
        }
        run_deliverable(deliverable, dfs, str(tmp_path), pipeline_db, run_date="2026-03-26")

        conn = duckdb.connect(pipeline_db)
        row = conn.execute(
            "SELECT row_count FROM report_runs WHERE report_name = 'daily_sales_validation'"
        ).fetchone()
        conn.close()
        assert row[0] == len(sales_df)

    def test_format_is_recorded(
        self, tmp_path, pipeline_db, sales_df, inventory_df, deliverables_config
    ):
        deliverable = deliverables_config["deliverables"][0]  # xlsx
        dfs = {"daily_sales_validation": sales_df, "inventory_validation": inventory_df}
        run_deliverable(deliverable, dfs, str(tmp_path), pipeline_db, run_date="2026-03-26")

        conn = duckdb.connect(pipeline_db)
        row = conn.execute("SELECT format FROM report_runs LIMIT 1").fetchone()
        conn.close()
        assert row[0] == "xlsx"


# ---------------------------------------------------------------------------
# Spec behavioral guarantee tests
# ---------------------------------------------------------------------------

class TestDeliverableGuarantees:

    def test_deliverable_not_blocked_by_validation_block(
        self, tmp_path, pipeline_db, sales_df, inventory_df, deliverables_config
    ):
        """Deliverables are produced even when validation_block has entries.

        Spec guarantee:
          'Deliverables (Extract): Not blocked by validation_block.'
          'validation_block — warns, does not block deliverables.'
        """
        from proto_pipe.io.db import init_all_pipeline_tables
        import duckdb as _duckdb

        # Write validation failures into validation_block
        conn = _duckdb.connect(pipeline_db)
        init_all_pipeline_tables(conn)
        conn.execute("""
            INSERT INTO validation_block
                (id, table_name, report_name, check_name, pk_value, reason, flagged_at)
            VALUES ('abc123', 'sales', 'daily_sales_validation',
                    'range_check', 'ORD-001', 'price out of range',
                    TIMESTAMPTZ '2026-04-01T00:00:00+00:00')
        """)
        conn.close()

        deliverable = deliverables_config["deliverables"][0]
        dfs = {
            "daily_sales_validation": sales_df,
            "inventory_validation": inventory_df,
        }
        # Must not raise and must produce output
        run_deliverable(deliverable, dfs, str(tmp_path), pipeline_db, run_date="2026-03-26")

        out_dir = Path(deliverable["output_dir"])
        files = list(out_dir.glob("*.xlsx"))
        assert len(files) == 1, (
            "Deliverable must be produced even when validation_block has entries"
        )

    def test_rerun_same_date_overwrites_file(
        self, tmp_path, pipeline_db, sales_df, inventory_df, deliverables_config
    ):
        """Re-running on the same date overwrites the output file.

        Spec guarantee:
          'Output files are never auto-deleted. Re-running on the same date
           overwrites the file.'
        """
        deliverable = deliverables_config["deliverables"][0]
        dfs = {
            "daily_sales_validation": sales_df,
            "inventory_validation": inventory_df,
        }

        run_deliverable(deliverable, dfs, str(tmp_path), pipeline_db, run_date="2026-03-26")
        out_dir = Path(deliverable["output_dir"])
        first_files = list(out_dir.glob("*.xlsx"))
        assert len(first_files) == 1
        mtime_first = first_files[0].stat().st_mtime

        import time
        time.sleep(0.05)  # ensure mtime can differ

        run_deliverable(deliverable, dfs, str(tmp_path), pipeline_db, run_date="2026-03-26")
        second_files = list(out_dir.glob("*.xlsx"))

        assert len(second_files) == 1, "Re-run must not create a second file"
        assert second_files[0].stat().st_mtime > mtime_first, (
            "Re-run on same date must overwrite the existing file"
        )

    def test_report_runs_records_deliverable_name_and_filename(
        self, tmp_path, pipeline_db, sales_df, inventory_df, deliverables_config
    ):
        """report_runs records deliverable_name, filename, and output_dir.

        Spec (adding_deliverables.md):
          'Every run is logged to the report_runs table in pipeline.db.
           Columns: deliverable_name, report_name, filename, output_dir,
           filters_applied, row_count, format, created_at.'
        """
        deliverable = deliverables_config["deliverables"][0]
        dfs = {
            "daily_sales_validation": sales_df,
            "inventory_validation": inventory_df,
        }
        run_deliverable(deliverable, dfs, str(tmp_path), pipeline_db, run_date="2026-03-26")

        conn = duckdb.connect(pipeline_db)
        row = conn.execute(
            "SELECT deliverable_name, filename, output_dir FROM report_runs LIMIT 1"
        ).fetchone()
        conn.close()

        assert row is not None
        assert row[0] is not None, "deliverable_name must be recorded in report_runs"
        assert row[1] is not None, "filename must be recorded in report_runs"
        assert row[2] is not None, "output_dir must be recorded in report_runs"
