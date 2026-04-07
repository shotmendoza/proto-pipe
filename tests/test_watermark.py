"""Tests for validation_pipeline.watermark.WatermarkStore

Covers:
- First run returns None
- set / get round-trip
- Advancing the watermark
- all() returns all stored watermarks
- Watermark is not advanced when a run fails (tested via runner integration)
"""

from datetime import datetime, timezone

from proto_pipe.pipelines.watermark import WatermarkStore

TS_A = datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
TS_B = datetime(2026, 2, 10,  9, 0, 0, tzinfo=timezone.utc)
TS_C = datetime(2026, 3,  1,  8, 30, 0, tzinfo=timezone.utc)


class TestWatermarkStore:
    def test_get_returns_none_on_first_run(self, watermark_store):
        assert watermark_store.get("daily_sales_validation") is None

    def test_set_and_get_round_trip(self, watermark_store):
        watermark_store.set("daily_sales_validation", TS_A)
        result = watermark_store.get("daily_sales_validation")
        assert result.replace(tzinfo=timezone.utc) == TS_A

    def test_advance_watermark(self, watermark_store):
        watermark_store.set("daily_sales_validation", TS_A)
        watermark_store.set("daily_sales_validation", TS_B)
        result = watermark_store.get("daily_sales_validation")
        assert result.replace(tzinfo=timezone.utc) == TS_B

    def test_multiple_reports_independent(self, watermark_store):
        watermark_store.set("report_a", TS_A)
        watermark_store.set("report_b", TS_B)
        assert watermark_store.get("report_a").replace(tzinfo=timezone.utc) == TS_A
        assert watermark_store.get("report_b").replace(tzinfo=timezone.utc) == TS_B

    def test_all_returns_all_watermarks(self, watermark_store):
        watermark_store.set("report_a", TS_A)
        watermark_store.set("report_b", TS_B)
        watermark_store.set("report_c", TS_C)
        all_marks = watermark_store.all()
        assert set(all_marks.keys()) == {"report_a", "report_b", "report_c"}

    def test_all_returns_empty_dict_when_no_marks(self, watermark_store):
        assert watermark_store.all() == {}

    def test_get_unknown_report_returns_none(self, watermark_store):
        watermark_store.set("report_a", TS_A)
        assert watermark_store.get("no_such_report") is None

    def test_watermark_persists_across_connections(self, watermark_db):
        store1 = WatermarkStore(watermark_db)
        store1.set("daily_sales_validation", TS_A)
        store2 = WatermarkStore(watermark_db)
        result = store2.get("daily_sales_validation")
        assert result.replace(tzinfo=timezone.utc) == TS_A


# ---------------------------------------------------------------------------
# Spec behavioral guarantee tests
# ---------------------------------------------------------------------------

class TestWatermarkGuarantees:

    def test_watermark_not_advanced_when_checks_fail(self, watermark_db, tmp_path):
        """Watermark must not advance when any check fails.

        Spec guarantee:
          'Watermarks advance only on full success.'
        """
        import duckdb
        import pandas as pd
        from proto_pipe.checks.built_in import check_range
        from proto_pipe.checks.registry import CheckRegistry
        from proto_pipe.reports.runner import run_report
        from functools import partial

        db_path = str(tmp_path / "pipeline.db")
        df = pd.DataFrame({
            "order_id": ["ORD-001", "ORD-002"],
            "price": [100.0, -5.0],
            "updated_at": pd.to_datetime(["2026-01-15", "2026-02-10"], utc=True),
        })
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE sales AS SELECT * FROM df")
        conn.close()

        reg = CheckRegistry()
        reg.register("price_range", partial(check_range, col="price", min_val=0, max_val=500))
        store = WatermarkStore(watermark_db)

        config = {
            "name": "sales_validation",
            "source": {
                "type": "duckdb", "path": db_path,
                "table": "sales", "timestamp_col": "updated_at",
            },
            "options": {"parallel": False},
            "resolved_checks": ["price_range"],
        }

        assert store.get("sales_validation") is None
        run_report(config, reg, store, pipeline_db=db_path)

        assert store.get("sales_validation") is None, (
            "Watermark must not advance when any check fails"
        )

    def test_watermark_uses_max_timestamp_from_data_not_wall_clock(
        self, watermark_db, tmp_path
    ):
        """Watermark reflects max(timestamp_col) from data, not wall-clock time.

        Spec guarantee:
          'Watermarks use max(timestamp_col) from data, not wall-clock time,
           and only advance when all checks pass.'
        """
        import duckdb
        import pandas as pd
        from proto_pipe.checks.built_in import check_nulls
        from proto_pipe.checks.registry import CheckRegistry
        from proto_pipe.reports.runner import run_report
        from datetime import datetime, timezone as tz

        db_path = str(tmp_path / "pipeline.db")
        max_ts = datetime(2026, 3, 1, 8, 30, 0, tzinfo=tz.utc)

        df = pd.DataFrame({
            "order_id": ["ORD-001", "ORD-002", "ORD-003"],
            "price": [99.99, 250.0, 15.50],
            "updated_at": pd.to_datetime(
                ["2026-01-15", "2026-02-10", "2026-03-01"], utc=True
            ),
        })
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE sales AS SELECT * FROM df")
        conn.close()

        reg = CheckRegistry()
        reg.register("null_check", check_nulls)
        store = WatermarkStore(watermark_db)

        config = {
            "name": "sales_validation",
            "source": {
                "type": "duckdb", "path": db_path,
                "table": "sales", "timestamp_col": "updated_at",
            },
            "options": {"parallel": False},
            "resolved_checks": ["null_check"],
        }

        before_run = datetime.now(tz.utc)
        run_report(config, reg, store, pipeline_db=db_path)

        mark = store.get("sales_validation")
        assert mark is not None
        assert mark.replace(tzinfo=tz.utc).date() == max_ts.date(), (
            f"Watermark must reflect max(timestamp_col) from data ({max_ts.date()}), "
            f"not wall-clock time ({before_run.date()})"
        )

    def test_get_returns_utc_normalized_datetime(self, watermark_store):
        """get() always returns a UTC-aware datetime.

        Spec guarantee: UTC consistency for watermark timestamps.
        """
        watermark_store.set("report_a", TS_A)
        result = watermark_store.get("report_a")
        assert result.tzinfo is not None, "Watermark must return a timezone-aware datetime"
        assert result.utcoffset().total_seconds() == 0, (
            "Watermark get() must return UTC-normalized datetime"
        )
