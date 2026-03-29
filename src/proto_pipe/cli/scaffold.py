"""Scaffold commands — new-source, new-report, new-check, new-deliverable."""
import inspect
import re
import uuid
from datetime import datetime, timezone
from difflib import SequenceMatcher
from graphlib import TopologicalSorter, CycleError
from pathlib import Path

import click
import duckdb
import questionary

from proto_pipe.cli.helpers import config_path_or_override
from proto_pipe.io.registry import load_config, write_config
from proto_pipe.registry.base import CheckRegistry

_PIPELINE_TABLES = {"flagged_rows", "ingest_log", "report_runs", "validation_flags"}


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


def _build_sql_scaffold(
    deliverable_name: str,
    selected_reports: list[str],
    reports_config: dict,
    sources_config: dict,
) -> str:
    """Build a SQL scaffold with JOIN stubs based on selected reports."""

    # Build a map of report name -> table name
    report_to_table = {
        r["name"]: r["source"]["table"] for r in reports_config.get("reports", [])
    }

    # Build a map of table name -> primary key from sources config
    table_to_pk = {
        s["target_table"]: s.get("primary_key")
        for s in sources_config.get("sources", [])
    }

    tables = [
        report_to_table.get(r) for r in selected_reports if report_to_table.get(r)
    ]

    if not tables:
        return f"-- {deliverable_name}.sql\nSELECT *\nFROM <table>;\n"

    base_table = tables[0]
    base_alias = "a"
    base_pk = table_to_pk.get(base_table)

    lines = [
        f"-- {deliverable_name}.sql",
        f"-- Deliverable query for: {deliverable_name}",
        f"--",
        f"-- Note: columns prefixed with _ are internal pipeline columns",
        f"-- (e.g. _ingested_at) and should be excluded from your SELECT.",
        f"--",
        f"-- Note: if joining on multiple columns, update the JOIN conditions below.",
        f"",
        f"SELECT",
        f"    {base_alias}.*",
    ]

    join_lines = []
    for i, table in enumerate(tables[1:], start=2):
        alias = chr(ord("a") + i - 1)
        pk = table_to_pk.get(table)
        lines.append(
            f"    -- , {alias}.<column>  -- add columns from {table} as needed"
        )

        if base_pk and pk:
            # Try matching primary keys
            if base_pk == pk:
                join_lines.append(
                    f"LEFT JOIN {table} {alias}\n"
                    f"    ON {base_alias}.{base_pk} = {alias}.{pk}"
                )
            else:
                join_lines.append(
                    f"LEFT JOIN {table} {alias}\n"
                    f"    ON {base_alias}.{base_pk} = {alias}.{pk}"
                    f"  -- update join keys if needed"
                )
        else:
            join_lines.append(
                f"LEFT JOIN {table} {alias}\n"
                f"    ON {base_alias}.<key> = {alias}.<key>  -- define join key"
            )

    lines.append(f"FROM {base_table} {base_alias}")
    lines.extend(join_lines)
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
        if t not in _PIPELINE_TABLES and t not in configured
    ]


def _get_check_params(check_name: str, check_registry: CheckRegistry) -> dict:
    """Return the params for a check by inspecting its function signature."""
    func = check_registry.get(check_name)
    if func is None:
        return {}
    sig = inspect.signature(func)
    return {
        name: param.default
        for name, param in sig.parameters.items()
        if name != "context"
    }


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
    src_cfg = config_path_or_override("sources_config", sources_config)
    inc_dir = config_path_or_override("incoming_dir", incoming_dir)

    config = load_config(src_cfg)
    existing_names = [s["name"] for s in config.get("sources", [])]

    click.echo("\n── New Source ──────────────────────────────")

    # Scan incoming dir
    files = _scan_incoming(inc_dir)
    if files:
        click.echo(f"\nFiles found in {inc_dir}:")
        for f in files:
            click.echo(f"  {f}")

        selected_file = questionary.select(
            "Which file are you configuring a source for?",
            choices=files + ["None of these — define manually"],
        ).ask()

        if selected_file == "None of these — define manually":
            selected_file = None
    else:
        click.echo(
            f"\nNo files found in '{inc_dir}'.\n"
            f"You can still define a source manually, or add files first.\n"
            f"You can also edit {src_cfg} directly."
        )
        selected_file = None

    # Name
    name = questionary.text("Source name (e.g. sales):").ask()
    if not name:
        click.echo("Cancelled.")
        return

    # Pattern — pre-fill from selected file
    suggested = _suggest_pattern(selected_file) if selected_file else "*.csv"
    pattern_input = questionary.text(
        "File pattern(s) — comma separated (e.g. sales_*.csv, Sales_*.xlsx):",
        default=suggested,
    ).ask()

    if name in existing_names:
        overwrite = questionary.confirm(
            f"Source '{name}' already exists. Edit it?"
        ).ask()
        if not overwrite:
            click.echo("Cancelled.")
            return

    # Pattern
    suggested = _suggest_pattern(files[0]) if files else "*.csv"
    pattern_input = questionary.text(
        "File pattern(s) — comma separated (e.g. sales_*.csv, Sales_*.xlsx):",
        default=suggested,
    ).ask()
    if not pattern_input:
        click.echo("Cancelled.")
        return
    patterns = [p.strip() for p in pattern_input.split(",")]

    # Target table
    table = questionary.text(
        "Target table name (press Enter to use source name):",
        default=name,
    ).ask()
    table = table.strip() if table else name

    # Primary key
    primary_key = questionary.text(
        "Primary key column (leave blank if none):",
    ).ask()
    primary_key = primary_key.strip() if primary_key else None

    if not primary_key:
        click.echo(
            "\n[warn] No primary key defined — all rows will be appended "
            "and duplicates won't be detected."
        )

    # on_duplicate — only ask if primary key is defined
    on_duplicate = None
    if primary_key:
        on_duplicate = questionary.select(
            "How should duplicate rows be handled?",
            choices=[
                questionary.Choice(
                    "flag   — flag conflicts for manual review (recommended)",
                    value="flag",
                ),
                questionary.Choice(
                    "upsert — replace existing row with incoming row", value="upsert"
                ),
                questionary.Choice(
                    "append — insert all rows, allow duplicates", value="append"
                ),
                questionary.Choice(
                    "skip   — keep existing row, ignore incoming", value="skip"
                ),
            ],
        ).ask()

    # Timestamp column
    timestamp_col = questionary.text(
        "Timestamp column name for incremental runs (leave blank to use _ingested_at):",
    ).ask()
    timestamp_col = timestamp_col.strip() if timestamp_col else None

    # Build source entry
    source = {
        "name": name,
        "patterns": patterns,
        "target_table": table,
    }
    if timestamp_col:
        source["timestamp_col"] = timestamp_col
    if primary_key:
        source["primary_key"] = primary_key
        source["on_duplicate"] = on_duplicate

    # Write to config
    sources = config.get("sources", [])
    if name in existing_names:
        sources = [s for s in sources if s["name"] != name]
    sources.append(source)
    config["sources"] = sources
    write_config(config, src_cfg)

    click.echo(f"\n[ok] Source '{name}' added to {src_cfg}")
    click.echo("\nNext steps:")
    click.echo("1. Review the entry in sources_config.yaml if needed")
    click.echo("2. Run: vp ingest")


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

    rep_cfg = config_path_or_override("reports_config", reports_config)
    p_db = config_path_or_override("pipeline_db", pipeline_db)

    config = load_config(rep_cfg)

    # Build a temporary registry with built-ins so we can inspect params
    check_registry = CheckRegistry()
    for name, func in BUILT_IN_CHECKS.items():
        check_registry.register(name, func)

    # Load custom checks if configured
    from proto_pipe.io.registry import load_custom_checks_module
    from proto_pipe.io.settings import load_settings
    settings = load_settings()
    module_path = settings.get("custom_checks_module")
    if module_path:
        load_custom_checks_module(module_path, check_registry)

    click.echo("\n── New Report ──────────────────────────────")

    # Step 1 — pick a table
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
    if not table:
        click.echo("Cancelled.")
        return

    # Step 2 — report name
    default_name = f"{table}_validation"
    name = questionary.text(
        "Report name:",
        default=default_name,
    ).ask()
    if not name:
        click.echo("Cancelled.")
        return

    # Check for existing report with same name
    existing_names = [r["name"] for r in config.get("reports", [])]
    if name in existing_names:
        overwrite = questionary.confirm(
            f"Report '{name}' already exists. Edit it?"
        ).ask()
        if not overwrite:
            click.echo("Cancelled.")
            return

    # Step 3 — multi-select checks
    available_checks = check_registry.available()
    if not available_checks:
        click.echo("\n  [warn] No checks available. Add built-in or custom checks first.")
        return

    selected_checks = questionary.checkbox(
        "Select checks to run on this report:",
        choices=available_checks,
    ).ask()
    if not selected_checks:
        click.echo("Cancelled.")
        return

    # Step 4 — optionally fill in params
    checks_with_params = {
        c: _get_check_params(c, check_registry)
        for c in selected_checks
        if _get_check_params(c, check_registry)
    }

    # For suggestion intelligence
    conn = duckdb.connect(p_db)
    check_entries = []
    if checks_with_params:
        fill_params = questionary.confirm(
            "Some checks have parameters. Would you like to fill them in now?"
        ).ask()

        for check_name in selected_checks:
            params = checks_with_params.get(check_name, {})
            if not params or not fill_params:
                entry = {"name": check_name}
                if params:
                    # Write param keys with blank values for manual fill-in
                    entry["params"] = {k: None for k in params}
                check_entries.append(entry)
                continue

            click.echo(f"\nParameters for '{check_name}':")
            table_cols = _get_table_columns(p_db, table)
            filled_params = {}

            for param_name, default in params.items():
                # Get suggestions from history
                suggestions = _get_param_suggestions(
                    conn, check_name, param_name, table_cols
                )

                if param_name == "col" or "col" in param_name.lower():
                    # Prioritize suggested columns, then show all
                    choices = suggestions + [
                        c for c in table_cols if c not in suggestions
                    ]

                    value = questionary.select(
                        f"{param_name}:",
                        choices=choices,
                    ).ask()

                elif isinstance(default, list):
                    choices = suggestions + [
                        c for c in table_cols if c not in suggestions
                    ]

                    value = questionary.checkbox(
                        f"  {param_name}:",
                        choices=choices,
                    ).ask()

                else:
                    # Use most recent suggestion as default if available
                    suggested_default = (
                        suggestions[0]
                        if suggestions
                        else (
                            str(default)
                            if default is not inspect.Parameter.empty
                            else ""
                        )
                    )

                    # Scalar param — free text
                    value = questionary.text(
                        f"{param_name}:",
                        default=suggested_default,
                    ).ask()

                    # Try to cast back to number if appropriate
                    if value:
                        try:
                            value = int(value) if "." not in value else float(value)
                        except ValueError:
                            pass

                filled_params[param_name] = value

            # Record to history
            _record_param_history(conn, check_name, name, table, filled_params)
            check_entries.append({"name": check_name, "params": filled_params})
    else:
        check_entries = [{"name": c} for c in selected_checks]
    conn.close()

    # Build report entry
    report = {
        "name": name,
        "source": {
            "type": "duckdb",
            "path": p_db,
            "table": table,
        },
        "options": {"parallel": False},
        "checks": check_entries,
    }

    # Write to config
    reports = config.get("reports", [])
    if name in existing_names:
        reports = [r for r in reports if r["name"] != name]
    reports.append(report)
    config["reports"] = reports
    write_config(config, rep_cfg)

    click.echo(f"\n[ok] Report '{name}' added to {rep_cfg}")
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
@click.command("new_view")
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
def scaffold_commands(cli):
    cli.add_command(new_source)
    cli.add_command(new_report)
    cli.add_command(new_deliverable)
    cli.add_command(new_view)
