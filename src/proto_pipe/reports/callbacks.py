"""Report execution callbacks — structured logging for the compute/write pipeline.

LogEntry captures structured progress data from _compute_report and
_apply_transforms_with_gate. The compute phase appends entries to
ReportBundle.log (thread-safe — each thread owns its bundle). The
orchestrator drains log entries through ReportCallback.on_log() on the
main thread between compute and write phases.

ReportCallback is a base class with no-op methods. The CLI layer
subclasses it (in cli/prompts.py) to render rich output. Business logic
modules never import the subclass — they accept the base type.

Design constraints:
  - reports/ never imports click, rich, or questionary (module responsibility rule)
  - _compute_report runs in ThreadPoolExecutor — no direct callback calls
  - _write_report and run_deliverable run sequentially — direct callback calls OK
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LogEntry:
    """Structured log entry produced during report computation.

    level: "info" | "warn" | "error"
    category: "pending" | "skip" | "check" | "transform" | "check_set_changed"
    report_name: which report produced this entry
    message: human-readable detail (display layer may reformat)
    check_name: optional — UUID of the check/transform involved
    count: optional — number of failures, pending records, etc.
    total: optional — total records for context (e.g., pending of total)
    """
    level: str
    category: str
    report_name: str
    message: str
    check_name: str | None = None
    count: int = 0
    total: int = 0


class ReportCallback:
    """Optional callback protocol for report execution progress.

    All methods are no-ops by default. CLI layer subclasses to render
    rich output. The reports layer only calls methods on this base type.

    on_log: called for compute-phase entries (drained from ReportBundle.log
            by the orchestrator on the main thread).
    on_write_done: called by _write_report after persisting to DuckDB.
    on_deliverable_written: called by run_deliverable after writing output file.
    """

    def on_log(self, entry: LogEntry) -> None:
        """Handle a structured log entry from the compute phase."""

    def on_write_done(
        self,
        report_name: str,
        target_table: str,
        row_count: int,
        first_run: bool,
    ) -> None:
        """Called after _write_report persists a report table."""

    def on_deliverable_written(self, path: str, row_count: int) -> None:
        """Called after run_deliverable writes an output file."""
