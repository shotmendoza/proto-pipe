"""Scaffold commands — new-source, new-report, new-check, new-deliverable."""
import inspect
import re
import uuid
from datetime import datetime, timezone
from difflib import SequenceMatcher
from graphlib import TopologicalSorter, CycleError
from pathlib import Path
from typing import Iterable

import click
import duckdb
import questionary

from proto_pipe.io.config import config_path_or_override, load_config
from proto_pipe.constants import PIPELINE_TABLES
from proto_pipe.checks.registry import CheckRegistry, CheckParamInspector


# ---------------------------------------------------------------------------
# New helpers for check display
# ---------------------------------------------------------------------------


def _get_check_first_sentence(func) -> str:
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


def _build_check_param_lines(
    check_name: str,
    check_registry: CheckRegistry,
    table_cols: list[str],
    alias_param_to_cols: dict[str, list[str]],
) -> list[str]:
    """Build param display lines for one check, showing column matches inline.

    DataFrame params are shown as auto-filled (no prompt needed).
    check_col / overwrite_cols appear as synthetic params when applicable.
    """
    original = _get_original_func(check_name, check_registry)
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

        elif _is_list_annotation(ann) or isinstance(param.default, list):
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


def _get_column_registry_hints(
    pipeline_db: str,
    columns: list[str],
) -> dict[str, dict[str, str]]:
    """Return {column_name: {source_name: declared_type}} from column_type_registry.

    Used by new_source to show which sources have already declared a type for
    each column, and to detect conflicts when multiple sources disagree.

    Falls back to {} if the registry table doesn't exist yet (before vp db-init)
    or if the DB file doesn't exist.

    :param pipeline_db: Path to the pipeline DuckDB file.
    :param columns:     Column names to look up.
    :return: Nested dict mapping column → source → type.
    """
    try:
        with duckdb.connect(pipeline_db) as conn:
            if not columns:
                return {}
            placeholders = ", ".join(["?"] * len(columns))
            rows = conn.execute(
                f"""
                SELECT column_name, source_name, declared_type
                FROM column_type_registry
                WHERE column_name IN ({placeholders})
                ORDER BY column_name, recorded_at DESC
            """,
                columns,
            ).fetchall()
    except Exception:
        return {}

    result: dict[str, dict[str, str]] = {}
    for col, source, dtype in rows:
        result.setdefault(col, {})[source] = dtype
    return result


def _filter_uningested(files: list[str], pipeline_db: str) -> list[str]:
    """Remove files already successfully logged in ingest_log."""
    try:
        from proto_pipe.io.db import get_ingested_filenames
        with duckdb.connect(pipeline_db) as conn:
            ingested_set = get_ingested_filenames(conn)
    except Exception:
        return files
    return [f for f in files if f not in ingested_set]


def _filter_unconfigured(files: list[str], sources: list[dict]) -> list[str]:
    """Return files that don't match any existing source pattern."""
    from fnmatch import fnmatch
    patterns = [p for s in sources for p in s.get("patterns", [])]
    return [f for f in files if not any(fnmatch(f, p) for p in patterns)]


def _infer_duckdb_type(series) -> str:
    """Return the DuckDB type string inferred from a pandas Series dtype."""
    import pandas as pd
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE"
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMPTZ"
    return "VARCHAR"


def _sorted_choices(items: list[str]) -> Iterable[str]:
    """Return items sorted case-insensitively for consistent prompt display."""
    return sorted(items, key=str.casefold)


def _similar_columns(param_value: str, columns: list[str], threshold: float = 0.6) -> list[str]:
    """Return columns similar to param_value, ranked by similarity."""
    substring = [c for c in columns if param_value.lower() in c.lower() or c.lower() in param_value.lower()]
    fuzzy = [
        c for c in columns
        if c not in substring
           and SequenceMatcher(None, param_value.lower(), c.lower()).ratio() >= threshold
    ]
    return substring + fuzzy


def _get_param_suggestions(
        conn: duckdb.DuckDBPyConnection,
        check_name: str,
        param_name: str,
        table_cols: list[str],
) -> list[str]:
    """Query check_params_history and return similar column suggestions."""
    try:
        rows = conn.execute("""
            SELECT DISTINCT param_value
            FROM check_params_history
            WHERE check_name = ? AND param_name = ?
            ORDER BY recorded_at DESC
            LIMIT 10
        """, [check_name, param_name]).fetchall()
    except Exception:
        return []

    suggestions = []
    seen = set()
    for (value,) in rows:
        if value:
            matches = _similar_columns(value, table_cols)
            for m in matches:
                if m not in seen:
                    suggestions.append(m)
                    seen.add(m)
    return suggestions


def _record_param_history(
    conn: duckdb.DuckDBPyConnection,
    check_name: str,
    report_name: str,
    table_name: str,
    params: dict,
) -> None:
    """Store used param values in check_params_history."""
    for param_name, value in params.items():
        if value is None:
            continue
        conn.execute("""
            INSERT INTO check_params_history
                (id, check_name, report_name, table_name, param_name, param_value, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            str(uuid.uuid4()),
            check_name,
            report_name,
            table_name,
            param_name,
            str(value),
            datetime.now(timezone.utc),
        ])


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


def _get_unconfigured_tables(pipeline_db: str, reports_config: dict) -> list[str]:
    """Return tables in the pipeline DB that aren't yet in reports_config."""
    configured = {r["source"]["table"] for r in reports_config.get("reports", [])}
    conn = duckdb.connect(pipeline_db)
    all_tables = conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main'
    """).df()["table_name"].tolist()
    conn.close()
    return [
        t for t in all_tables
        if t not in PIPELINE_TABLES and t not in configured
    ]


def _get_table_columns(pipeline_db: str, table: str) -> list[str]:
    """Return column names for a table, excluding internal pipeline columns."""
    conn = duckdb.connect(pipeline_db)
    cols = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        [table],
    ).df()["column_name"].tolist()
    conn.close()
    return [c for c in cols if not c.startswith("_")]


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


def _is_list_annotation(ann) -> bool:
    """True if annotation is list or list[str] or similar."""
    if ann is list:
        return True
    return getattr(ann, "__origin__", None) is list


def _get_original_func(check_name: str, check_registry: CheckRegistry):
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


def _get_check_params(check_name: str, check_registry: CheckRegistry) -> dict:
    """Return the promptable params for a check.

    DataFrame params are excluded — they are auto-filled with the report
    table at call time and never shown to the user.
    """
    func = _get_original_func(check_name, check_registry)
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


def _fill_params(
    selected_checks: list[str],
    table: str,
    p_db: str,
    check_registry: CheckRegistry,
    multi_select: bool,
    conn: "duckdb.DuckDBPyConnection",
    report_name: str,
    existing_alias_map: list[dict] | None = None,
) -> tuple[list[dict], list[dict], bool]:
    """Fill params for each selected check, building alias_map entries for column params.

    Column params → alias_map (not in params dict).
    Scalar params → params dict.
    DataFrame params → skipped (auto-filled at runtime).
    check_col (kind=check + returns DataFrame) → params dict.
    overwrite_cols (kind=transform + returns DataFrame) → params dict.

    Returns (check_entries, alias_map_entries, go_back).
    """
    table_cols = sorted(_get_table_columns(p_db, table))
    existing_alias_map = existing_alias_map or []

    alias_param_to_cols: dict[str, list[str]] = {}
    for entry in existing_alias_map:
        alias_param_to_cols.setdefault(entry["param"], []).append(entry["column"])

    accumulated_alias: list[dict] = list(existing_alias_map)

    checks_with_params = {
        c: _get_check_params(c, check_registry)
        for c in selected_checks
        if _get_check_params(c, check_registry)
    }

    # Also include checks that have no promptable params but need df-return prompts
    df_return_checks = set()
    for check_name in selected_checks:
        original = _get_original_func(check_name, check_registry)
        if original:
            inspector = CheckParamInspector(original)
            if inspector.has_dataframe_input() and inspector.returns_dataframe():
                df_return_checks.add(check_name)

    checks_needing_prompts = set(checks_with_params.keys()) | df_return_checks

    if not checks_needing_prompts:
        return [{"name": c} for c in selected_checks], accumulated_alias, False

    check_entries = []

    for check_name in selected_checks:
        params = checks_with_params.get(check_name, {})
        original = _get_original_func(check_name, check_registry)
        inspector = CheckParamInspector(original) if original else None

        has_df_return = check_name in df_return_checks
        kind = check_registry.get_kind(check_name)

        if not params and not has_df_return:
            check_entries.append({"name": check_name})
            continue

        click.echo(f"\nParameters for '{check_name}':")

        eligible = (
            multi_select
            and inspector is not None
            and inspector.is_multiselect_eligible()
        )
        col_params = inspector.column_params() if inspector else []
        sig = inspect.signature(inspector.func) if inspector else None

        filled_params: dict = {}

        for param_name, default in params.items():
            ann = (
                sig.parameters[param_name].annotation
                if sig and param_name in sig.parameters
                else inspect.Parameter.empty
            )

            if param_name in col_params:
                alias_cols = alias_param_to_cols.get(param_name, [])
                if alias_cols:
                    choices = alias_cols + [
                        c for c in table_cols if c not in alias_cols
                    ]
                else:
                    history = _get_param_suggestions(
                        conn, check_name, param_name, table_cols
                    )
                    choices = history + [c for c in table_cols if c not in history]

                if eligible:
                    click.echo(
                        f"  ℹ  Selecting multiple columns will run '{check_name}'"
                        f" once per column."
                    )
                    value = questionary.checkbox(
                        f"{param_name}:", choices=sorted(choices)
                    ).ask()
                    if value is None:
                        return [], [], True
                    value = sorted(value)
                    for col in value:
                        if not any(
                            e["param"] == param_name and e["column"] == col
                            for e in accumulated_alias
                        ):
                            accumulated_alias.append(
                                {"param": param_name, "column": col}
                            )
                else:
                    value = questionary.select(f"{param_name}:", choices=choices).ask()
                    if value is None:
                        return [], [], True
                    if not any(
                        e["param"] == param_name and e["column"] == value
                        for e in accumulated_alias
                    ):
                        accumulated_alias.append({"param": param_name, "column": value})

                alias_param_to_cols[param_name] = [
                    e["column"] for e in accumulated_alias if e["param"] == param_name
                ]

            elif _is_list_annotation(ann) or isinstance(default, list):
                suggestions = _get_param_suggestions(
                    conn, check_name, param_name, table_cols
                )
                choices = suggestions + [c for c in table_cols if c not in suggestions]
                value = questionary.checkbox(
                    f"{param_name}:", choices=sorted(choices)
                ).ask()
                if value is None:
                    return [], [], True
                filled_params[param_name] = sorted(value)

            else:
                suggestions = _get_param_suggestions(
                    conn, check_name, param_name, table_cols
                )
                suggested_default = (
                    suggestions[0]
                    if suggestions
                    else (
                        str(default) if default is not inspect.Parameter.empty else ""
                    )
                )
                value = questionary.text(
                    f"{param_name}:", default=suggested_default
                ).ask()
                if value is None:
                    return [], [], True
                if value:
                    try:
                        value = int(value) if "." not in value else float(value)
                    except ValueError:
                        pass
                filled_params[param_name] = value

        # ── DataFrame-return prompts ──────────────────────────────────────────
        if has_df_return:
            if kind == "check":
                # Prompt for the boolean column in the returned DataFrame
                click.echo(
                    f"\n  ℹ  '{check_name}' returns a DataFrame. Select the column "
                    f"that contains the boolean pass/fail values."
                )
                check_col = questionary.select(
                    "check_col — boolean column in the returned DataFrame:",
                    choices=table_cols,
                ).ask()
                if check_col is None:
                    return [], [], True
                filled_params["check_col"] = check_col

            elif kind == "transform":
                # Prompt for columns to overwrite in the source table
                click.echo(
                    f"\n  ℹ  '{check_name}' returns a DataFrame. Select the columns "
                    f"from the returned DataFrame that should overwrite the table."
                )
                overwrite_cols = questionary.checkbox(
                    "overwrite_cols — columns to write back to the table:",
                    choices=table_cols,
                ).ask()
                if overwrite_cols is None:
                    return [], [], True
                filled_params["overwrite_cols"] = sorted(overwrite_cols)

        _record_param_history(conn, check_name, report_name, table, filled_params)
        entry = {"name": check_name}
        if filled_params:
            entry["params"] = filled_params
        check_entries.append(entry)

    return check_entries, accumulated_alias, False


# ---------------------------------------------------------------------------
# vp new-report
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Deliverable Scaffolding
# ----------------------------------------------------------


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
@click.command("table-reset")
@click.option("--report", "report_name", default=None, help="Report name to reset.")
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def table_reset(report_name, reports_config, pipeline_db):
    """Drop a report table and clear its ingest history so it re-ingests cleanly.

    This is a destructive operation. The table is dropped from DuckDB and all
    ingest_log entries for it are removed. The next `vp ingest` run recreates
    the table from source files from scratch, including re-applying transforms.

    \b
    Example:
      vp table-reset --report us_carrier
    """
    from proto_pipe.io.ingest import reset_report

    rep_cfg = config_path_or_override("reports_config", reports_config)
    p_db = config_path_or_override("pipeline_db", pipeline_db)

    config = load_config(rep_cfg)
    available_reports = [r["name"] for r in config.get("reports", [])]

    if not available_reports:
        click.echo("  No reports configured. Run: vp new-report")
        return

    if not report_name:
        report_name = questionary.select(
            "Which report do you want to reset?",
            choices=available_reports,
        ).ask()
        if not report_name:
            click.echo("Cancelled.")
            return

    if report_name not in available_reports:
        click.echo(f"  [error] Report '{report_name}' not found in {rep_cfg}")
        return

    report_cfg = next(r for r in config["reports"] if r["name"] == report_name)
    table_name = report_cfg["source"]["table"]

    click.echo(f"\n  Report:  {report_name}")
    click.echo(f"  Table:   {table_name}")
    click.echo(f"  DB:      {p_db}")
    click.echo()

    confirmed = questionary.confirm(
        f"Drop '{table_name}' and clear its ingest history? This cannot be undone.",
        default=False,
    ).ask()
    if not confirmed:
        click.echo("Cancelled.")
        return

    reset_report(table_name, p_db)
    click.echo(f"\n[ok] '{table_name}' reset. Run: vp ingest")


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


def _build_rich_sql_scaffold(
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


# ---------------------------------------------------------------------------
# vp new-sql
# ---------------------------------------------------------------------------


def scaffold_commands(cli):
    """Register scaffold commands that haven't moved to action groups yet."""
    cli.add_command(table_reset)
