"""Scaffold commands — new-source, new-report, new-check, new-deliverable."""
import inspect
import re
from graphlib import TopologicalSorter, CycleError
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import click
import duckdb
import questionary

from proto_pipe.io.config import config_path_or_override, load_config
from proto_pipe.checks.registry import CheckRegistry, CheckParamInspector


def glob_most_recent(search_dir: str, *patterns: str) -> "Path | None":
    """Return the most recently modified file matching any glob pattern.

    Used by flagged/validated retry to find the latest export CSV.
    Returns None if no files match any pattern.
    """
    inc = Path(search_dir)
    candidates: list[Path] = []
    for pat in patterns:
        candidates.extend(inc.glob(pat))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


# ---------------------------------------------------------------------------
# New helpers for check display
# ---------------------------------------------------------------------------


def get_check_first_sentence(func) -> str:
    """Extract and return the first sentence from a function's docstring.

    Used as the description shown alongside each check in vp new-report.
    Returns an empty string if no docstring is present.
    """
    doc = inspect.getdoc(func)
    if not doc:
        return ""
    # Split on period-space, period-newline, or bare newline — take first chunk
    sentence = re.split(r"\.\s|\.\n|\n", doc)[0].strip()
    if sentence and not sentence.endswith("."):
        sentence += "."
    return sentence


def build_check_param_lines(
    check_name: str,
    check_registry: CheckRegistry,
    table_cols: list[str],
    alias_param_to_cols: dict[str, list[str]],
) -> list[str]:
    """Build param display lines for one check, showing column matches inline.

    DataFrame params are shown as auto-filled (no prompt needed).
    check_col / overwrite_cols appear as synthetic params when applicable.
    """
    original = get_original_func(check_name, check_registry)
    if original is None:
        return []

    inspector = CheckParamInspector(original)
    col_params = set(inspector.column_params())
    df_params = set(inspector.dataframe_params())
    sig = inspect.signature(inspector.func)

    # Determine if this is a df-returning check or transform
    kind = check_registry.get_kind(check_name)
    returns_df = inspector.returns_dataframe()

    lines = []
    for param_name, param in sig.parameters.items():
        if param_name == "context":
            continue

        ann = param.annotation

        if param_name in df_params:
            lines.append(f"    {param_name:<18} dataframe  (auto: full table)")

        elif param_name in col_params:
            alias_cols = alias_param_to_cols.get(param_name, [])
            other_cols = [c for c in table_cols if c not in alias_cols]
            all_cols = alias_cols + other_cols

            if not all_cols:
                lines.append(f"    {param_name:<18} column   (no columns in table)")
            elif len(all_cols) <= 3:
                lines.append(f"    {param_name:<18} column   → {', '.join(all_cols)}")
            else:
                shown = ", ".join(all_cols[:3])
                remaining = len(all_cols) - 3
                lines.append(f"    {param_name:<18} column   → {shown}  ({remaining}+)")

        elif is_list_annotation(ann) or isinstance(param.default, list):
            lines.append(f"    {param_name:<18} list")

        else:
            lines.append(f"    {param_name:<18} scalar")

    # Show synthetic df-return params
    if returns_df and df_params:
        if kind == "check":
            lines.append(
                f"    {'check_col':<18} column   (boolean column in returned DataFrame)"
            )
        elif kind == "transform":
            lines.append(
                f"    {'overwrite_cols':<18} columns  (columns to overwrite in table)"
            )

    return lines


def filter_uningested(files: list[str], pipeline_db: str) -> list[str]:
    """Remove files already successfully logged in ingest_state."""
    try:
        from proto_pipe.io.db import get_ingested_filenames
        with duckdb.connect(pipeline_db) as conn:
            ingested_set = get_ingested_filenames(conn)
    except Exception:
        return files
    return [f for f in files if f not in ingested_set]


def filter_unconfigured(files: list[str], sources: list[dict]) -> list[str]:
    """Return files that don't match any existing source pattern."""
    from fnmatch import fnmatch
    patterns = [p for s in sources for p in s.get("patterns", [])]
    return [f for f in files if not any(fnmatch(f, p) for p in patterns)]


def _sorted_choices(items: list[str]) -> Iterable[str]:
    """Return items sorted case-insensitively for consistent prompt display."""
    return sorted(items, key=str.casefold)


def _detect_view_dependencies(sql: str, known_views: list[str]) -> list[str]:
    """Return names of known views referenced in the SQL."""
    return [v for v in known_views if re.search(rf"\b{v}\b", sql)]


def _sort_views(views: list[dict]) -> list[dict]:
    """Topologically sort views by their SQL dependencies.

    Raises click.ClickException if a circular dependency is detected.
    """
    known_names = [v["name"] for v in views]
    view_map = {v["name"]: v for v in views}

    graph = {}
    for view in views:
        sql_path = Path(view["sql_file"])
        if sql_path.exists():
            sql = sql_path.read_text()
            deps = _detect_view_dependencies(sql, known_names)
            # exclude self-references just in case
            graph[view["name"]] = {d for d in deps if d != view["name"]}
        else:
            graph[view["name"]] = set()

    try:
        sorter = TopologicalSorter(graph)
        sorted_names = list(sorter.static_order())
    except CycleError as e:
        raise click.ClickException(
            f"Circular dependency detected between views: {e}\n"
            f"  Fix: check your SQL files and remove the circular reference."
        )

    return [view_map[n] for n in sorted_names if n in view_map]


def _format_join_clause(
        base_alias: str,
        table: str,
        alias: str,
        base_pk: str | None,
        pk: str | None,
) -> str:
    """Build the LEFT JOIN clause for a table."""
    if base_pk and pk:
        join_condition = f"{base_alias}.{base_pk} = {alias}.{pk}"
        if base_pk != pk:
            return (
                f"LEFT JOIN {table} {alias}\n"
                f"    ON {join_condition}  -- update join keys if needed"
            )
        return (
            f"LEFT JOIN {table} {alias}\n"
            f"    ON {join_condition}"
        )

    return (
        f"LEFT JOIN {table} {alias}\n"
        f"    ON {base_alias}.<key> = {alias}.<key>  -- define join key"
    )


def _suggest_pattern(filename: str) -> str:
    """Strip numbers and dates from a filename to suggest a glob pattern.

    Replaces the first contiguous block of digits/separators with *.
    e.g. sales_2024_01_15.csv -> sales_*.csv
    """
    stem = Path(filename).stem
    suffix = Path(filename).suffix
    pattern = re.sub(r"\d[\d_\-]*", "*", stem, count=1)
    # Clean up any double wildcards
    pattern = re.sub(r"\*+", "*", pattern)
    return f"{pattern}{suffix}"


def _group_files_by_pattern(files: list[str]) -> dict[str, list[str]]:
    """Group files by their suggested glob pattern.

    Returns {pattern: [file, ...]} — files within each group are sorted.
    Useful for showing the user one entry per pattern rather than per file.

    Example:
        ["sales_2026-01.csv", "sales_2026-02.csv", "inventory_2026-01.csv"]
        → {"sales_*.csv": ["sales_2026-01.csv", "sales_2026-02.csv"],
           "inventory_*.csv": ["inventory_2026-01.csv"]}
    """
    groups: dict[str, list[str]] = {}
    for f in files:
        pattern = _suggest_pattern(f)
        groups.setdefault(pattern, []).append(f)
    for pattern in groups:
        groups[pattern] = sorted(groups[pattern])
    return groups


def _scan_incoming(incoming_dir: str) -> list[str]:
    """Return supported filenames from the incoming directory."""
    supported = {".csv", ".xlsx", ".xls"}
    p = Path(incoming_dir)
    if not p.exists():
        return []
    return [
        f.name
        for f in sorted(p.iterdir())
        if f.is_file() and f.suffix.lower() in supported
    ]


# ---------------------------------------------------------------------------
# vp new-source
# ---------------------------------------------------------------------------


def is_list_annotation(ann) -> bool:
    """True if annotation is list or list[str] or similar."""
    if ann is list:
        return True
    return getattr(ann, "__origin__", None) is list


def get_original_func(check_name: str, check_registry: CheckRegistry):
    """Return the original unwrapped function from the registry."""
    import functools
    func = check_registry.get(check_name)
    if func is None:
        return None
    unwrapped = func
    while isinstance(unwrapped, functools.partial):
        unwrapped = unwrapped.func
    unwrapped = inspect.unwrap(unwrapped)
    while isinstance(unwrapped, functools.partial):
        unwrapped = unwrapped.func
    return unwrapped


def get_check_params(check_name: str, check_registry: CheckRegistry) -> dict:
    """Return the promptable params for a check.

    DataFrame params are excluded — they are auto-filled with the report
    table at call time and never shown to the user.
    """
    func = get_original_func(check_name, check_registry)
    if func is None:
        return {}
    inspector = CheckParamInspector(func)
    df_params = set(inspector.dataframe_params())
    sig = inspect.signature(func)
    return {
        name: param.default
        for name, param in sig.parameters.items()
        if name != "context" and name not in df_params
    }


# ---------------------------------------------------------------------------
# vp new-report
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Deliverable Scaffolding
# ----------------------------------------------------------


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
def _scan_macros(macros_dir: str) -> list[str]:
    """Scan macros_dir and return a list of macro signatures found in .sql files.

    Extracts names and param lists from CREATE MACRO statements so the scaffold
    can show the user what's available to call inline.
    """
    import re

    p = Path(macros_dir)
    if not p.exists():
        return []

    signatures = []
    for sql_file in sorted(p.glob("*.sql")):
        try:
            text = sql_file.read_text()
            # Match: CREATE [OR REPLACE] MACRO name(params) AS
            matches = re.findall(
                r"CREATE\s+(?:OR\s+REPLACE\s+)?MACRO\s+(\w+\([^)]*\))",
                text,
                re.IGNORECASE,
            )
            signatures.extend(matches)
        except Exception:
            pass
    return signatures


def build_rich_sql_scaffold(
        deliverable_name: str,
        selected_reports: list[str],
        reports_config: dict,
        sources_config: dict,
        macros_dir: str | None = None,
) -> str:
    """Build an annotated SQL scaffold with join stubs, macro references,
    and transform notes.

    Extends _build_sql_scaffold with:
    - Header notes about transforms already applied to the tables
    - List of available macros the user can call inline
    - Inline column comments for joined tables
    """
    report_to_table = {
        report["name"]: report["source"]["table"]
        for report in reports_config.get("reports", [])
    }
    table_to_pk = {
        source["target_table"]: source.get("primary_key")
        for source in sources_config.get("sources", [])
    }

    selected_tables = [
        report_to_table.get(report_name)
        for report_name in selected_reports
        if report_to_table.get(report_name)
    ]

    if not selected_tables:
        return f"-- {deliverable_name}.sql\nSELECT *\nFROM <table>;\n"

    macro_signatures = _scan_macros(macros_dir) if macros_dir else []
    base_table = selected_tables[0]
    base_alias = "a"
    base_pk = table_to_pk.get(base_table)

    lines = [
        f"-- {deliverable_name}.sql",
        f"-- Deliverable query for: {deliverable_name}",
        f"--",
        f"-- The tables below have transforms applied before this query runs.",
        f"-- Any @custom_check(kind='transform') functions registered for",
        f"-- these reports are already reflected in the data.",
        f"--",
        f"-- Columns prefixed with _ are internal pipeline columns",
        f"-- (e.g. _ingested_at) — exclude from your SELECT.",
        f"--",
        f"-- If joining on multiple columns, update the JOIN conditions below.",
        "",
        "SELECT",
        f"    {base_alias}.*",
    ]

    if macro_signatures:
        lines.append("    -- Example macro usage (uncomment and adapt):")
        for signature in macro_signatures:
            macro_name = signature.split("(")[0]
            lines.append(f"    -- , {macro_name}({base_alias}.<col>) AS <col>")
    else:
        lines.append(
            f"    -- , macro_name({base_alias}.<col>) AS <col>"
            f"  -- call a registered macro inline"
        )

    lines.append(f"FROM {base_table} {base_alias}")

    for index, table in enumerate(selected_tables[1:], start=2):
        alias = chr(ord("a") + index - 1)
        pk = table_to_pk.get(table)

        lines.append(f"    -- , {alias}.<column>  -- add columns from {table} as needed")
        lines.append(
            _format_join_clause(
                base_alias=base_alias,
                table=table,
                alias=alias,
                base_pk=base_pk,
                pk=pk,
            )
        )

    lines.append(f"WHERE {base_alias}._ingested_at >= '<from_date>'")
    lines.append(f"ORDER BY {base_alias}._ingested_at DESC;")
    return "\n".join(lines)


@dataclass
class JoinSpec:
    """Describes how two report tables are joined in a deliverable SQL query.

    Used by build_deliverable_sql to produce real JOIN clauses.
    Prompted by DeliverableConfigPrompter.prompt_joins().
    """
    left_table: str
    right_table: str
    left_key: str
    right_key: str
    join_type: str = "LEFT"  # LEFT, INNER, FULL OUTER


@dataclass
class DeliverableSQLSpec:
    """Complete specification for generating a deliverable SQL file.

    Built by DeliverableConfigPrompter.run(), consumed by build_deliverable_sql().
    """
    deliverable_name: str
    report_columns: dict[str, list[str]]  # report_name → selected columns
    join_specs: list[JoinSpec] = field(default_factory=list)
    order_by: str | None = None


def build_deliverable_sql(spec: DeliverableSQLSpec) -> str:
    """Build a complete, runnable SQL file from a DeliverableSQLSpec.

    Single report: plain SELECT with no aliases.
    Multiple reports: aliased tables with JOIN clauses from spec.join_specs.
    """
    reports = list(spec.report_columns.keys())

    if not reports:
        return (
            f"-- {spec.deliverable_name}.sql\n"
            f"-- Generated by: vp new deliverable\n"
            f"SELECT *\nFROM <table>;\n"
        )

    header = [
        f"-- {spec.deliverable_name}.sql",
        f"-- Generated by: vp new deliverable",
        f"-- Reports: {', '.join(reports)}",
        "",
    ]

    if len(reports) == 1:
        return _build_single_report_sql(spec, header)
    return _build_multi_report_sql(spec, header)


def _build_single_report_sql(spec: DeliverableSQLSpec, header: list[str]) -> str:
    """SELECT for a single report — no aliases needed."""
    report = list(spec.report_columns.keys())[0]
    columns = spec.report_columns[report]

    lines = header[:]
    lines.append("SELECT")
    lines.append(_format_column_list(columns, alias=None))
    lines.append(f"FROM {report}")

    if spec.order_by:
        lines.append(f"ORDER BY {spec.order_by}")

    lines.append(";")
    return "\n".join(lines) + "\n"


def _build_multi_report_sql(spec: DeliverableSQLSpec, header: list[str]) -> str:
    """SELECT with JOINs for multiple reports."""
    reports = list(spec.report_columns.keys())

    # Assign aliases: a, b, c, ...
    aliases: dict[str, str] = {}
    for i, report in enumerate(reports):
        aliases[report] = chr(ord("a") + i)

    # Build join lookup: right_table → JoinSpec
    join_lookup: dict[str, JoinSpec] = {}
    for js in spec.join_specs:
        join_lookup[js.right_table] = js

    base_report = reports[0]
    base_alias = aliases[base_report]

    lines = header[:]
    lines.append("SELECT")

    # Collect all column references with aliases
    col_refs: list[str] = []
    for report in reports:
        alias = aliases[report]
        for col in spec.report_columns[report]:
            col_refs.append(f"{alias}.{col}")

    lines.append(_format_column_list(col_refs, alias=None))
    lines.append(f"FROM {base_report} {base_alias}")

    # Add JOINs for each subsequent report
    for report in reports[1:]:
        alias = aliases[report]
        js = join_lookup.get(report)
        if js:
            left_alias = aliases[js.left_table]
            join_kw = f"{js.join_type} JOIN"
            lines.append(
                f"{join_kw} {report} {alias}\n"
                f"    ON {left_alias}.{js.left_key} = {alias}.{js.right_key}"
            )
        else:
            # Fallback — shouldn't happen if wizard ran correctly
            lines.append(
                f"LEFT JOIN {report} {alias}\n"
                f"    ON {base_alias}.<key> = {alias}.<key>  -- define join key"
            )

    if spec.order_by:
        # Qualify order_by with base alias
        lines.append(f"ORDER BY {base_alias}.{spec.order_by}")

    lines.append(";")
    return "\n".join(lines) + "\n"


def _format_column_list(columns: list[str], alias: str | None) -> str:
    """Format columns as indented, comma-separated lines for a SELECT clause."""
    if not columns:
        return "    *"

    formatted = []
    for i, col in enumerate(columns):
        ref = f"{alias}.{col}" if alias else col
        if i == 0:
            formatted.append(f"    {ref}")
        else:
            formatted.append(f"    , {ref}")
    return "\n".join(formatted)