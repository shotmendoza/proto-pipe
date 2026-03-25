"""
Entrypoint — wire everything together and run.

Flow:
  1. Ingest CSV/Excel files from a directory into DuckDB (pipelines.ingest)
  2. Register checks and reports from config (registry)
  3. Run all reports against DuckDB (watermark-filtered)
"""
from pathlib import Path

from src.io.ingest import ingest_directory
from src.reports.runner import run_all_reports
from src.pipelines.watermark import WatermarkStore
from src.io.registry import load_config, register_from_config
from src.registry.base import check_registry, report_registry


def main():
    # 1. Load config [Report and Checks]
    _root = Path(__file__).resolve().parent
    sources_config = load_config(_root / "config" / "sources_config.yaml")
    reports_config = load_config(_root / "config" / "reports_config.yaml")

    ingest_directory(
        directory="data/incoming/",
        sources=sources_config["sources"],
        db_path="data/pipeline.db",
        mode="append",
    )

    # 2. Load report config and register checks + reports
    #    After this: check_registry.run(name, context) needs no param knowledge
    #                report_registry.all() has everything the runner needs
    register_from_config(
        config=reports_config,
        check_registry=check_registry,
        report_registry=report_registry
    )

    # 3. Set up watermark store (DuckDB table)
    watermark_store = WatermarkStore(db_path=_root / "data" / "watermarks.db")

    # 4. Run all reports in parallel
    results = run_all_reports(
        report_registry=report_registry,
        check_registry=check_registry,
        watermark_store=watermark_store,
        parallel_reports=True,
    )

    # 5. Print summary
    for report in results:
        status = report["status"]
        name = report["report"]
        print(f"\n=== {name} [{status}] ===")
        if status == "completed":
            for check_name, outcome in report["results"].items():
                print(f"  {check_name}: {outcome['status']}")
                if outcome["status"] == "failed":
                    print(f"error: {outcome['error']}")
                else:
                    print(f"result: {outcome['result']}")


if __name__ == "__main__":
    main()
