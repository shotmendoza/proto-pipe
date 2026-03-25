
"""Tests for validation_pipeline.watermark.WatermarkStore

Covers:
- First run returns None
- set / get round-trip
- Advancing the watermark
- all() returns all stored watermarks
- Watermark is not advanced when a run fails (tested via runner integration)
"""

from datetime import datetime, timezone
from src.pipelines.watermark import WatermarkStore


TS_A = datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
TS_B = datetime(2026, 2, 10,  9, 0, 0, tzinfo=timezone.utc)
TS_C = datetime(2026, 3,  1,  8, 30, 0, tzinfo=timezone.utc)


class TestWatermarkStore:
    def test_get_returns_none_on_first_run(self, watermark_store):
        assert watermark_store.get("daily_sales_validation") is None

    def test_set_and_get_round_trip(self, watermark_store):
        watermark_store.set("daily_sales_validation", TS_A)
        result = watermark_store.get("daily_sales_validation")
        # Compare UTC-normalised datetimes
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
        """Watermark written through one store instance is readable by another."""
        store1 = WatermarkStore(watermark_db)
        store1.set("daily_sales_validation", TS_A)

        store2 = WatermarkStore(watermark_db)
        result = store2.get("daily_sales_validation")
        assert result.replace(tzinfo=timezone.utc) == TS_A
