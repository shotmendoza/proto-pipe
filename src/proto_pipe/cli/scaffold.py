"""Scaffold commands — new-source, new-report, new-check, new-deliverable."""
import inspect
import re
import uuid
from datetime import datetime, timezone
from difflib import SequenceMatcher
from graphlib import TopologicalSorter, CycleError
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
from typing import Iterable

import click
import duckdb
import questionary

from proto_pipe.cli.helpers import config_path_or_override
from proto_pipe.constants import PIPELINE_TABLES
from proto_pipe.io.registry import load_config, write_config
from proto_pipe.registry.base import CheckRegistry


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

    For column params: shows alias_map cols first, then remaining table cols,
    capped at 3 with (N+) suffix if more exist.
    For scalar/list params: shows the param kind only.

    :param check_name:         Registry name of the check.
    :param check_registry:     CheckRegistry to look up the function.
    :param table_cols:         Sorted column names for the target table.
    :param alias_param_to_cols: {param: [col, ...]} from the accumulated alias_map.
    :return: List of formatted lines, one per param.
    """
    from proto_pipe.checks.inspector import CheckParamInspector

    original = _get_original_func(check_name, check_registry)
    if original is None:
        return []

    inspector = CheckParamInspector(original)
    col_params = set(inspector.column_params())
    sig = inspect.signature(inspector.func)

    lines = []
    for param_name, param in sig.parameters.items():
        if param_name == "context":
            continue

        ann = param.annotation

        if param_name in col_params:
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


def _build_sql_scaffold(
        deliverable_name: str,
        selected_reports: list[str],
        reports_config: dict,
        sources_config: dict,
) -> str:
    """Build a SQL scaffold with JOIN stubs based on selected reports."""
    report_name_to_table = {
        report["name"]: report["source"]["table"]
        for report in reports_config.get("reports", [])
    }
    table_to_primary_key = {
        source["target_table"]: source.get("primary_key")
        for source in sources_config.get("sources", [])
    }

    selected_tables = [
        report_name_to_table[report_name]
        for report_name in selected_reports
        if report_name in report_name_to_table
    ]

    if not selected_tables:
        return f"-- {deliverable_name}.sql\nSELECT *\nFROM <table>;\n"

    base_table = selected_tables[0]
    base_alias = "a"
    base_pk = table_to_primary_key.get(base_table)

    lines = [
        f"-- {deliverable_name}.sql",
        f"-- Deliverable query for: {deliverable_name}",
        f"--",
        f"-- Note: columns prefixed with _ are internal pipeline columns",
        f"-- (e.g. _ingested_at) and should be excluded from your SELECT.",
        f"--",
        f"-- Note: if joining on multiple columns, update the JOIN conditions below.",
        "",
        "SELECT",
        f"    {base_alias}.*",
    ]

    join_clauses: list[str] = []
    for index, table in enumerate(selected_tables[1:], start=2):
        alias = chr(ord("a") + index - 1)
        pk = table_to_primary_key.get(table)

        lines.append(
            f"    -- , {alias}.<column>  -- add columns from {table} as needed"
        )
        join_clauses.append(
            _format_join_clause(
                base_alias=base_alias,
                table=table,
                alias=alias,
                base_pk=base_pk,
                pk=pk,
            )
        )

    lines.append(f"FROM {base_table} {base_alias}")
    lines.extend(join_clauses)
    lines.append(f"WHERE {base_alias}._ingested_at >= '<from_date>'")
    lines.append(f"ORDER BY {base_alias}._ingested_at DESC;")
    return "\n".join(lines)


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
@click.command("new-source")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--incoming-dir", default=None, help="Override incoming directory path.")
def new_source(sources_config, incoming_dir):
    """Interactively define a new data source and add it to sources_config.yaml.

    \b
    Example:
      vp new-source
    """
    from fnmatch import fnmatch
    from proto_pipe.cli.helpers import config_path_or_override
    from proto_pipe.io.ingest import load_file
    from proto_pipe.io.settings import load_settings

    src_cfg = config_path_or_override("sources_config", sources_config)
    inc_dir = config_path_or_override("incoming_dir", incoming_dir)
    settings = load_settings()
    pipeline_db = settings["paths"]["pipeline_db"]

    config = SourceConfig(src_cfg)
    existing_names = config.names()

    click.echo("\n── New Source ──────────────────────────────")

    # Scan incoming dir — hide already-ingested files
    files = _scan_incoming(inc_dir)
    files = _filter_uningested(files, pipeline_db)

    sample = None
    selected_file = None

    if files:
        selected_file_choice = questionary.select(
            "Which file are you configuring a source for?",
            choices=files + ["None of these — define manually"],
        ).ask()

        if selected_file_choice == "None of these — define manually":
            selected_file = None
        elif selected_file_choice:
            selected_file = selected_file_choice
            suggested_pattern = _suggest_pattern(selected_file)
            matching = [f for f in files if fnmatch(f, suggested_pattern)]
            if len(matching) > 1:
                click.echo(
                    f"\n  {len(matching)} files match the pattern '{suggested_pattern}'"
                    f" — they will all be ingested into the same table.\n"
                )
    else:
        click.echo(
            f"\nNo files found in '{inc_dir}'.\n"
            f"You can still define a source manually, or add files first.\n"
            f"You can also edit {src_cfg} directly."
        )

    # Load sample for column inspection
    if selected_file:
        try:
            sample = load_file(Path(inc_dir) / selected_file)
        except Exception:
            pass

    # Load registry hints for column type display
    registry_hints: dict = {}
    if sample is not None:
        file_cols = [c for c in sample.columns if not c.startswith("_")]
        try:
            with duckdb.connect(pipeline_db) as conn:
                registry_hints = get_registry_hints(conn, file_cols)
        except Exception:
            pass

    prompter = SourceConfigPrompter(
        sample_df=sample,
        registry_hints=registry_hints,
    )

    # Name
    name = prompter.prompt_name(existing_names)
    if not name:
        click.echo("Cancelled.")
        return

    # Pattern
    suggested = _suggest_pattern(selected_file) if selected_file else "*.csv"
    patterns = prompter.prompt_pattern(suggested)
    if not patterns:
        click.echo("Cancelled.")
        return

    # Target table
    table = prompter.prompt_target_table(default=name)
    if not table:
        click.echo("Cancelled.")
        return

    # Primary key
    primary_key = prompter.prompt_primary_key()
    if not primary_key:
        click.echo(
            "\n  [warn] No primary key defined — all rows will be appended"
            " and duplicates won't be detected."
        )

    # on_duplicate + timestamp — only when PK defined
    on_duplicate = None
    timestamp_col = None
    if primary_key:
        on_duplicate = prompter.prompt_on_duplicate()
        if on_duplicate is None:
            click.echo("Cancelled.")
            return
        timestamp_col = prompter.prompt_timestamp_col()

    # Column type confirmation
    confirmed_types = prompter.prompt_column_types()

    # Write confirmed types to registry
    if confirmed_types:
        try:
            with duckdb.connect(pipeline_db) as conn:
                write_registry_types(conn, name, confirmed_types)
        except Exception:
            pass

    # Build source entry — no column_types in config, registry is source of truth
    source: dict = {
        "name": name,
        "patterns": patterns,
        "target_table": table,
    }
    if timestamp_col:
        source["timestamp_col"] = timestamp_col
    if primary_key:
        source["primary_key"] = primary_key
        source["on_duplicate"] = on_duplicate

    config.add_or_update(source)

    click.echo(f"\n[ok] Source '{name}' added to {src_cfg}")
    click.echo("\nNext steps:")
    click.echo("  1. Review the entry in sources_config.yaml if needed")
    click.echo("  2. Run: vp ingest")


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
    """Return the params for a check by inspecting its original function signature."""
    func = _get_original_func(check_name, check_registry)
    if func is None:
        return {}
    sig = inspect.signature(func)
    return {
        name: param.default
        for name, param in sig.parameters.items()
        if name != "context"
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

    Column params are resolved through alias_map — they are NOT written into the
    check's params dict. Scalar params are written into the check's params dict.

    Always prompts for params — the user can review and change any mapping
    including ones already present in the alias_map.

    Returns (check_entries, alias_map_entries, go_back).
      check_entries — list of {name, params} dicts. Column params omitted.
      alias_map_entries — accumulated list of {param, column} dicts.
      go_back — True if the user pressed ESC.
    """
    from proto_pipe.checks.inspector import CheckParamInspector

    table_cols = sorted(_get_table_columns(p_db, table))
    existing_alias_map = existing_alias_map or []

    # Build {param: [col, ...]} from existing alias_map for suggestions and dedup.
    alias_param_to_cols: dict[str, list[str]] = {}
    for entry in existing_alias_map:
        alias_param_to_cols.setdefault(entry["param"], []).append(entry["column"])

    accumulated_alias: list[dict] = list(existing_alias_map)

    checks_with_params = {
        c: _get_check_params(c, check_registry)
        for c in selected_checks
        if _get_check_params(c, check_registry)
    }

    if not checks_with_params:
        return [{"name": c} for c in selected_checks], accumulated_alias, False

    check_entries = []

    for check_name in selected_checks:
        params = checks_with_params.get(check_name, {})

        if not params:
            check_entries.append({"name": check_name})
            continue

        click.echo(f"\nParameters for '{check_name}':")

        original = _get_original_func(check_name, check_registry)
        inspector = CheckParamInspector(original)
        eligible = multi_select and inspector.is_multiselect_eligible()
        col_params = inspector.column_params()
        sig = inspect.signature(inspector.func)

        filled_params: dict = {}  # scalar params only

        for param_name, default in params.items():
            ann = (
                sig.parameters[param_name].annotation
                if param_name in sig.parameters
                else inspect.Parameter.empty
            )

            if param_name in col_params:
                # Alias_map cols first (pre-mapped from this session), then table cols
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
                        f"{param_name}:",
                        choices=sorted(choices),
                    ).ask()
                    if value is None:
                        return [], [], True  # ESC -> go back
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
                    value = questionary.select(
                        f"{param_name}:",
                        choices=choices,
                    ).ask()
                    if value is None:
                        return [], [], True  # ESC -> go back
                    if not any(
                        e["param"] == param_name and e["column"] == value
                        for e in accumulated_alias
                    ):
                        accumulated_alias.append({"param": param_name, "column": value})

                # Refresh lookup for subsequent checks in this session
                alias_param_to_cols[param_name] = [
                    e["column"] for e in accumulated_alias if e["param"] == param_name
                ]
                # Column params are NOT added to filled_params — they live in alias_map

            elif _is_list_annotation(ann) or isinstance(default, list):
                suggestions = _get_param_suggestions(
                    conn, check_name, param_name, table_cols
                )
                choices = suggestions + [c for c in table_cols if c not in suggestions]
                value = questionary.checkbox(
                    f"{param_name}:",
                    choices=sorted(choices),
                ).ask()
                if value is None:
                    return [], [], True  # ESC -> go back
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
                    f"{param_name}:",
                    default=suggested_default,
                ).ask()
                if value is None:
                    return [], [], True  # ESC -> go back
                if value:
                    try:
                        value = int(value) if "." not in value else float(value)
                    except ValueError:
                        pass
                filled_params[param_name] = value

        _record_param_history(conn, check_name, report_name, table, filled_params)
        entry = {"name": check_name}
        if filled_params:
            entry["params"] = filled_params
        check_entries.append(entry)

    return check_entries, accumulated_alias, False


# ---------------------------------------------------------------------------
# New command: vp check-func
# ---------------------------------------------------------------------------


@click.command("check-func")
@click.option(
    "--custom-checks", default=None, help="Override custom_checks_module path."
)
def check_func(custom_checks):
    """Inspect all check functions and report any that failed validation.

    Loads built-in and custom checks fresh into a temporary registry and
    prints a structured report: which passed, which failed, and exactly why.

    Checks marked ✗ are silently skipped during vp new-report. Run this
    command after adding or changing custom checks to confirm they registered.

    \b
    Example:
      vp check-func
      vp check-func --custom-checks path/to/my_checks.py
    """
    from proto_pipe.checks.built_in import BUILT_IN_CHECKS
    from proto_pipe.checks.helpers import _DECORATED_CHECKS
    from proto_pipe.io.settings import load_settings
    from proto_pipe.registry.base import CheckRegistry as _TempRegistry

    settings = load_settings()
    module_path = custom_checks or settings.get("custom_checks_module")

    # Snapshot built-in names before register_custom_check can add to BUILT_IN_CHECKS
    builtin_names: set[str] = set(BUILT_IN_CHECKS.keys())

    # Fresh temporary registry — never touches the global instance
    tmp = _TempRegistry()

    for name, func in BUILT_IN_CHECKS.items():
        tmp.register(name, func, kind="check")

    # Load custom checks
    custom_names: set[str] = set()
    custom_load_error: str | None = None

    if module_path:
        path = Path(module_path)
        if not path.exists():
            custom_load_error = (
                f"File not found: '{module_path}'. "
                f"Check the custom_checks_module path in pipeline.yaml."
            )
        else:
            _DECORATED_CHECKS.clear()
            spec = spec_from_file_location("_custom_checks_diag", path)
            module = module_from_spec(spec)
            try:
                spec.loader.exec_module(module)
            except Exception as exc:
                custom_load_error = (
                    f"Failed to import '{module_path}': {exc}\n"
                    f"       Fix: resolve the import error in your custom checks file."
                )

            if not custom_load_error:
                if not _DECORATED_CHECKS:
                    custom_load_error = (
                        f"Loaded '{module_path}' but found no @custom_check decorated functions. "
                        f"Add @custom_check to each function you want registered."
                    )
                else:
                    custom_names = set(_DECORATED_CHECKS.keys())
                    for name, (func, kind) in _DECORATED_CHECKS.items():
                        tmp.register(name, func, kind=kind)

    # ── Display ───────────────────────────────────────────────────────────────
    ok = set(tmp.available())
    bad = tmp.failed()  # {name: reason}

    click.echo("\n── Check Functions ─────────────────────────\n")

    for label, names in [("Built-in", builtin_names), ("Custom", custom_names)]:
        if label == "Custom" and not module_path:
            continue

        if label == "Custom" and custom_load_error:
            click.echo(f"Custom checks ({module_path}):")
            click.echo(f"  ✗  Module error: {custom_load_error}")
            click.echo()
            continue

        group_ok = sorted(n for n in names if n in ok)
        group_bad = {n: r for n, r in bad.items() if n in names}
        total = len(group_ok) + len(group_bad)

        click.echo(f"{label} checks ({total}):")
        for name in group_ok:
            kind = tmp.get_kind(name)
            click.echo(f"  ✓  {name}  [{kind}]")
        for name, reason in sorted(group_bad.items()):
            click.echo(f"  ✗  {name}")
            click.echo(f"       {reason}")
        click.echo()

    # ── Summary ───────────────────────────────────────────────────────────────
    total_ok = len(ok)
    total_bad = len(bad) + (1 if custom_load_error else 0)

    parts = [f"{total_ok} passed"]
    if total_bad:
        parts.append(f"{total_bad} failed")
    click.echo(f"Summary: {', '.join(parts)}")

    if total_bad:
        click.echo(
            "\n  Checks marked ✗ are silently skipped during 'vp new-report'."
            "\n  Fix the issues above and run 'vp check-func' again to confirm."
        )


# ---------------------------------------------------------------------------
# vp new-report
# ---------------------------------------------------------------------------
@click.command("new-report")
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def new_report(reports_config, pipeline_db):
    """Interactively define a new report and add it to reports_config.yaml.

    \b
    Example:
      vp new-report
    """
    from proto_pipe.registry.base import CheckRegistry
    from proto_pipe.checks.built_in import BUILT_IN_CHECKS
    from proto_pipe.cli.helpers import config_path_or_override
    from proto_pipe.io.registry import load_custom_checks_module
    from proto_pipe.io.settings import load_settings
    from proto_pipe.checks.inspector import CheckParamInspector
    from proto_pipe.io.db import init_check_registry_metadata

    rep_cfg = config_path_or_override("reports_config", reports_config)
    p_db = config_path_or_override("pipeline_db", pipeline_db)

    config = load_config(rep_cfg)
    settings = load_settings()
    multi_select = settings.get("multi_select_params", True)

    # Build a temporary registry with built-ins so we can inspect params
    check_registry = CheckRegistry()
    for name, func in BUILT_IN_CHECKS.items():
        check_registry.register(name, func)

    module_path = settings.get("custom_checks_module")
    if module_path:
        load_custom_checks_module(module_path, check_registry)

    available_checks = check_registry.available()
    if not available_checks:
        click.echo("\n[warn] No checks available. Add built-in or custom checks first.")
        return

    click.echo("\n── New Report ──────────────────────────────")
    click.echo("  Press ESC at any prompt to go back to the previous step.\n")

    STEP_TABLE = 0
    STEP_NAME = 1
    STEP_CHECKS = 2
    STEP_PARAMS = 3
    STEP_DONE = 4

    state: dict = {}
    step = STEP_TABLE

    conn = duckdb.connect(p_db)

    init_check_registry_metadata(conn)
    for check_name in check_registry.available():
        original = _get_original_func(check_name, check_registry)
        if original is not None:
            CheckParamInspector(original).write_to_db(conn, check_name)

    try:
        while step < STEP_DONE:

            # ── Table ──────────────────────────────────────────────────────────
            if step == STEP_TABLE:
                available_tables = _get_unconfigured_tables(p_db, config)
                if not available_tables:
                    click.echo(
                        "\n  No unconfigured tables found. Either all tables already have "
                        "reports defined, or no tables have been ingested yet.\n"
                        "  Run: vp ingest   to load files first."
                    )
                    return

                table = questionary.select(
                    "Which table should this report run against?",
                    choices=available_tables,
                ).ask()
                if table is None:
                    click.echo("Cancelled.")
                    return
                state["table"] = table
                step = STEP_NAME

            # ── Name ───────────────────────────────────────────────────────────
            elif step == STEP_NAME:
                default_name = f"{state['table']}_validation"
                name = questionary.text(
                    "Report name:",
                    default=default_name,
                ).ask()
                if name is None:
                    step = STEP_TABLE
                    continue

                existing_names = [r["name"] for r in config.get("reports", [])]
                if name in existing_names:
                    overwrite = questionary.confirm(
                        f"Report '{name}' already exists. Edit it?"
                    ).ask()
                    if overwrite is None:
                        step = STEP_TABLE
                        continue
                    if not overwrite:
                        click.echo("Cancelled.")
                        return

                state["name"] = name
                state["existing_names"] = existing_names
                step = STEP_CHECKS

            # ── Check selection ────────────────────────────────────────────────
            elif step == STEP_CHECKS:
                table_cols = sorted(_get_table_columns(p_db, state["table"]))

                # Build alias_map lookup from accumulated state
                alias_param_to_cols: dict[str, list[str]] = {}
                for entry in state.get("alias_map", []):
                    alias_param_to_cols.setdefault(entry["param"], []).append(
                        entry["column"]
                    )

                # Print full summary before the checkbox
                click.echo("\n  Available checks:\n")
                choices = []
                for check_name in available_checks:
                    original = _get_original_func(check_name, check_registry)
                    first_sentence = (
                        _get_check_first_sentence(original) if original else ""
                    )
                    param_lines = _build_check_param_lines(
                        check_name, check_registry, table_cols, alias_param_to_cols
                    )

                    click.echo(f"  {check_name}")
                    if first_sentence:
                        click.echo(f"    {first_sentence}")
                    for line in param_lines:
                        click.echo(line)
                    click.echo()

                    choices.append(
                        questionary.Choice(
                            title=check_name,
                            value=check_name,
                            description=first_sentence,
                        )
                    )

                selected = questionary.checkbox(
                    "Select checks to run on this report:",
                    choices=choices,
                ).ask()
                if selected is None:
                    step = STEP_NAME
                    continue
                if not selected:
                    click.echo("  Please select at least one check.")
                    continue
                state["selected_checks"] = selected
                step = STEP_PARAMS

            # ── Param filling ──────────────────────────────────────────────────
            elif step == STEP_PARAMS:
                check_entries, alias_map_entries, go_back = _fill_params(
                    selected_checks=state["selected_checks"],
                    table=state["table"],
                    p_db=p_db,
                    check_registry=check_registry,
                    multi_select=multi_select,
                    conn=conn,
                    report_name=state["name"],
                    existing_alias_map=state.get("alias_map", []),
                )
                if go_back:
                    step = STEP_CHECKS
                    continue
                state["check_entries"] = check_entries
                state["alias_map"] = alias_map_entries
                step = STEP_DONE

    finally:
        conn.close()

    # Build and write report entry
    report = {
        "name": state["name"],
        "source": {
            "type": "duckdb",
            "path": p_db,
            "table": state["table"],
        },
        "options": {"parallel": False},
    }
    if state.get("alias_map"):
        report["alias_map"] = state["alias_map"]
    report["checks"] = state["check_entries"]

    reports = config.get("reports", [])
    if state["name"] in state["existing_names"]:
        reports = [r for r in reports if r["name"] != state["name"]]
    reports.append(report)
    config["reports"] = reports
    write_config(config, rep_cfg)

    click.echo(f"\n[ok] Report '{state['name']}' added to {rep_cfg}")
    click.echo("\nNext steps:")
    click.echo("1. Review the entry in reports_config.yaml if needed")
    click.echo("2. Run: vp validate")


@click.command("new-deliverable")
@click.option(
    "--deliverables-config", default=None, help="Override deliverables config path."
)
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--sql-dir", default=None, help="Override SQL directory path.")
def new_deliverable(deliverables_config, reports_config, sources_config, sql_dir):
    """Interactively define a new deliverable and add it to deliverables_config.yaml.

    \b
    Example:
      vp new-deliverable
    """
    from proto_pipe.cli.helpers import config_path_or_override
    from proto_pipe.io.settings import load_settings

    del_cfg = config_path_or_override("deliverables_config", deliverables_config)
    rep_cfg = config_path_or_override("reports_config", reports_config)
    src_cfg = config_path_or_override("sources_config", sources_config)
    settings = load_settings()
    sql_directory = sql_dir or settings["paths"].get("sql_dir", "sql")

    del_config = load_config(del_cfg)
    rep_config = load_config(rep_cfg)
    src_config = load_config(src_cfg)

    existing_names = [d["name"] for d in del_config.get("deliverables", [])]
    available_reports = [r["name"] for r in rep_config.get("reports", [])]

    if not available_reports:
        click.echo("\n  No reports configured yet. Run: vp new-report")
        return

    click.echo("\n── New Deliverable ─────────────────────────")

    # Step 1 — name
    name = questionary.text("Deliverable name (e.g. carrier_a):").ask()
    if not name:
        click.echo("Cancelled.")
        return

    if name in existing_names:
        overwrite = questionary.confirm(
            f"Deliverable '{name}' already exists. Edit it?"
        ).ask()
        if not overwrite:
            click.echo("Cancelled.")
            return

    # Step 2 — format
    fmt = questionary.select(
        "Output format:",
        choices=[
            questionary.Choice(
                "xlsx — Excel file (supports multiple tabs)", value="xlsx"
            ),
            questionary.Choice("csv  — CSV file (single output)", value="csv"),
        ],
    ).ask()
    if not fmt:
        click.echo("Cancelled.")
        return

    # Step 3 — filename template
    default_filename = f"{name}_{{date}}.{fmt}"
    filename_template = questionary.text(
        "Filename template:",
        default=default_filename,
    ).ask()
    if not filename_template:
        click.echo("Cancelled.")
        return

    # Step 4 — select reports
    selected_reports = questionary.checkbox(
        "Select reports to include in this deliverable:",
        choices=available_reports,
    ).ask()
    if not selected_reports:
        click.echo("Cancelled.")
        return

    # Warn if CSV with multiple reports
    if fmt == "csv" and len(selected_reports) > 1:
        click.echo(
            "\n  [warn] Multiple reports selected for a CSV deliverable —"
            " you'll need to join them in your SQL file."
        )

    # Step 5 — SQL transformation
    use_sql = questionary.confirm(
        "Does this deliverable need a custom SQL transformation?",
        default=True,
    ).ask()

    sql_file = None
    if use_sql:
        sql_path = Path(sql_directory) / f"{name}.sql"
        sql_path.parent.mkdir(parents=True, exist_ok=True)

        if sql_path.exists():
            click.echo(f"  [skip] {sql_path} already exists — not overwriting")
        else:
            scaffold = _build_sql_scaffold(
                name, selected_reports, rep_config, src_config
            )
            sql_path.write_text(scaffold)
            click.echo(f"  [ok]   {sql_path}")

        sql_file = str(sql_path)

    # Step 6 — optionally fill in details
    report_entries = []
    fill_details = questionary.confirm(
        "Would you like to fill in sheet names and date filters now?",
        default=False,
    ).ask()

    for report_name in selected_reports:
        entry = {"name": report_name}

        if fmt == "xlsx":
            if fill_details:
                sheet = questionary.text(
                    f"Sheet name for '{report_name}':",
                    default=report_name,
                ).ask()
                entry["sheet"] = sheet or report_name
            else:
                entry["sheet"] = report_name

        if fill_details:
            add_filter = questionary.confirm(
                f"Add a date filter for '{report_name}'?",
                default=False,
            ).ask()
            if add_filter:
                date_col = questionary.text(
                    f"Date column to filter on:",
                    default="_ingested_at",
                ).ask()
                from_date = questionary.text("From date (YYYY-MM-DD):").ask()
                to_date = questionary.text(
                    "To date (YYYY-MM-DD, leave blank for open-ended):"
                ).ask()
                date_filter = {"col": date_col, "from": from_date}
                if to_date:
                    date_filter["to"] = to_date
                entry["filters"] = {"date_filters": [date_filter]}

        report_entries.append(entry)

    # Build deliverable entry
    deliverable = {
        "name": name,
        "format": fmt,
        "filename_template": filename_template,
        "reports": report_entries,
    }
    if sql_file:
        deliverable["sql_file"] = sql_file

    # Write to config
    deliverables = del_config.get("deliverables", [])
    if name in existing_names:
        deliverables = [d for d in deliverables if d["name"] != name]
    deliverables.append(deliverable)
    del_config["deliverables"] = deliverables
    write_config(del_config, del_cfg)

    click.echo(f"\n[ok] Deliverable '{name}' added to {del_cfg}")
    click.echo("\nNext steps:")
    if use_sql:
        click.echo(f"1. Edit {sql_file} with your transformation query")
        click.echo(f"2. Run: vp pull-report --deliverable {name}")
    else:
        click.echo(f"1. Run: vp pull-report --deliverable {name}")


# ---------------------------------------------------------------------------
# Deliverable Scaffolding
# ----------------------------------------------------------
@click.command("new-view")
@click.option("--name", required=True, help="Name for the new deliverable (e.g. carrier_a_report).")
@click.option("--output-dir", default=None, help="Override SQL output directory.")
def new_view(name: str, output_dir: str):
    """Scaffold a starter SQL file for a new deliverable view and sql transformations.

    \b
    Example:
      vp new-deliverable --name carrier_a_report
    """
    from proto_pipe.io.settings import load_settings

    settings = load_settings()
    sql_dir = Path(output_dir or settings["paths"].get("sql_dir", "sql"))
    sql_dir.mkdir(parents=True, exist_ok=True)

    dest = sql_dir / f"{name}.sql"
    if dest.exists():
        click.echo(f"  [skip] {dest} already exists (delete it first to regenerate)")
        return

    template = f"""\
-- {name}.sql
-- Deliverable query for: {name}
--
-- This query defines what gets written to the output file.
-- Use DuckDB SQL syntax. Reference ingested tables directly by name.
-- Date filtering should be written inline here — no placeholders needed.
--
-- Example:
--   SELECT order_id, customer_id, price, region
--   FROM sales
--   WHERE updated_at >= '2024-01-01'
--     AND region = 'West'
--   ORDER BY updated_at DESC;

SELECT *
FROM <table>
WHERE <date_col> >= '<from_date>'
ORDER BY <date_col> DESC;
"""

    dest.write_text(template)
    click.echo(f"[ok] {dest}")
    click.echo(f"\nNext steps:")
    click.echo(f"1. Edit {dest} with your query")
    click.echo(f"2. Add an entry in deliverables_config.yaml referencing {name}.sql")
    click.echo(f"3. Run: vp pull-report --deliverable {name}")


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


@click.command("new-macro")
@click.argument("name")
@click.option("--macros-dir", default=None, help="Override macros directory.")
def new_macro(name: str, macros_dir: str | None):
    """Scaffold a new SQL macro file in the macros directory.

    Creates a templated .sql file with a CREATE OR REPLACE MACRO stub.
    Registers macros_dir in pipeline.yaml if not already set.

    \b
    Example:
      vp new-macro normalize_transaction_type
    """
    from proto_pipe.io.settings import load_settings, DEFAULT_SETTINGS_PATH

    settings = load_settings()
    macro_dir_path = macros_dir or settings.get("macros_dir", "macros")

    dest_dir = Path(macro_dir_path)
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest = dest_dir / f"{name}.sql"
    if dest.exists():
        click.echo(f"  [skip] {dest} already exists — delete it first to regenerate")
        return

    template = f"""\
-- {name}.sql
-- Macro: describe what this macro does
--
-- Macros are registered at pipeline startup and available in all
-- view and deliverable SQL queries.
--
-- Use CREATE OR REPLACE so re-running vp db-init is idempotent.

CREATE OR REPLACE MACRO {name}(val) AS
    CASE
        WHEN val = 'old_value' THEN 'new_value'
        ELSE val
    END;
"""
    dest.write_text(template)
    click.echo(f"[ok] {dest}")

    # Add macros_dir to pipeline.yaml if not already present
    pipeline_yaml = DEFAULT_SETTINGS_PATH
    if pipeline_yaml.exists():
        doc = load_config(pipeline_yaml)
        if "macros_dir" not in doc:
            doc["macros_dir"] = macro_dir_path
            write_config(doc, pipeline_yaml)
            click.echo(f"[ok] Added macros_dir = '{macro_dir_path}' to pipeline.yaml")

    click.echo(f"\nNext steps:")
    click.echo(f"1. Edit {dest} with your macro logic")
    click.echo(f"2. Run: vp db-init   (re-registers all macros)")


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
@click.command("new-sql")
@click.argument("name", required=False)
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
@click.option("--sql-dir", default=None, help="Override SQL output directory.")
def new_sql(name, reports_config, sources_config, sql_dir):
    """Scaffold an annotated SQL file for a deliverable query.

    Creates a .sql file with join stubs, macro references, and inline comments
    so you can write your carrier queries faster. Does not modify any config
    file — wire it up in deliverables_config.yaml when ready.

    \\b
    Example:
      vp new-sql carrier_a_sales
      vp new-sql # interactive — prompts for name and tables
    """
    from proto_pipe.cli.helpers import config_path_or_override
    from proto_pipe.io.settings import load_settings

    rep_cfg = config_path_or_override("reports_config", reports_config)
    src_cfg = config_path_or_override("sources_config", sources_config)
    settings = load_settings()

    sql_directory = sql_dir or settings["paths"].get("sql_dir", "sql")
    macros_dir = settings.get("macros_dir", "macros")

    rep_config = load_config(rep_cfg)
    src_config = load_config(src_cfg)

    available_reports = [r["name"] for r in rep_config.get("reports", [])]

    if not available_reports:
        click.echo("  No reports configured yet. Run: vp new-report")
        return

    click.echo("\n── New SQL File ────────────────────────────")

    # Name
    if not name:
        name = questionary.text("SQL file name (e.g. carrier_a_sales):").ask()
        if not name:
            click.echo("Cancelled.")
            return

    dest = Path(sql_directory) / f"{name}.sql"
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists():
        overwrite = questionary.confirm(f"{dest} already exists. Overwrite?").ask()
        if not overwrite:
            click.echo("Cancelled.")
            return

    # Select reports / tables
    selected = questionary.checkbox(
        "Which reports should this query pull from?",
        choices=available_reports,
    ).ask()
    if not selected:
        click.echo("Cancelled.")
        return

    scaffold = _build_rich_sql_scaffold(
        deliverable_name=name,
        selected_reports=selected,
        reports_config=rep_config,
        sources_config=src_config,
        macros_dir=macros_dir,
    )

    dest.write_text(scaffold)
    click.echo(f"\n[ok] {dest}")

    # Show which macros were found
    macros = _scan_macros(macros_dir)
    if macros:
        click.echo(f"\nMacros available in this query:")
        for sig in macros:
            click.echo(f"  {sig}")

    click.echo(f"\nNext steps:")
    click.echo(f"1. Edit {dest} with your query logic")
    click.echo(f"2. Reference it in deliverables_config.yaml:\n" f'sql_file: "{dest}"')


def scaffold_commands(cli):
    cli.add_command(new_source)
    cli.add_command(new_report)
    cli.add_command(new_deliverable)
    cli.add_command(new_view)
    cli.add_command(table_reset)
    cli.add_command(new_macro)
    cli.add_command(new_sql)
    cli.add_command(check_func)
