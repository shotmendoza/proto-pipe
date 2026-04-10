"""vp view — view pipeline resource data."""

from pathlib import Path

import click
import duckdb
import questionary

from proto_pipe.cli.commands.table import get_reviewer
from proto_pipe.io.config import config_path_or_override
from proto_pipe.constants import PIPELINE_TABLES
from proto_pipe.io.db import get_all_tables
from proto_pipe.pipelines.query import query_table_with_status


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _show_or_export(
    df, title: str, export: str | None, pk_col: str | None = None
) -> None:
    """Route a DataFrame to the reviewer or export it depending on --export flag."""
    if export == "csv":
        from proto_pipe.io.config import load_settings
        from datetime import datetime, timezone

        settings = load_settings()
        out_dir = settings["paths"]["output_dir"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        safe_title = title.split("(")[0].strip().replace(" ", "_").replace("/", "_")
        output_path = Path(out_dir) / f"{safe_title}_{today}.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        click.echo(f"[ok] {len(df)} row(s) exported to: {output_path}")
    elif export == "custom":
        from proto_pipe.cli.prompts import prompt_custom_export_path

        custom_path = prompt_custom_export_path()
        if custom_path:
            custom_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(custom_path, index=False)
            click.echo(f"[ok] {len(df)} row(s) exported to: {custom_path}")
        else:
            # User cancelled the path prompt — fall back to rich display
            reviewer = get_reviewer(edit=False)
            reviewer.show(df, title=title, pk_col=pk_col)
    else:
        reviewer = get_reviewer(edit=False)
        reviewer.show(df, title=title, pk_col=pk_col)


def _resolve_name(
    arg: str | None,
    opt: str | None,
    choices: list[str],
    prompt: str,
) -> str | None:
    """Return a resolved name from argument, option, or interactive select."""
    name = arg or opt
    if name:
        return name
    if not choices:
        return None
    return questionary.select(prompt, choices=choices).ask()


# ---------------------------------------------------------------------------
# vp view group
# ---------------------------------------------------------------------------

@click.group("view", context_settings={"max_content_width": 120})
def view_cmd():
    """View pipeline resource data.

    \b
    Examples:
      vp view source
      vp view source --table sales
      vp view report
      vp view deliverable
      vp view table
    """
    pass


# ---------------------------------------------------------------------------
# vp view report
# ---------------------------------------------------------------------------


@view_cmd.command("report")
@click.argument("report_name", required=False)
@click.option("--report", default=None, help="Report name.")
@click.option(
    "--export",
    default=None,
    type=click.Choice(["csv", "custom"]),
    help="csv: auto-named file in output_dir. custom: prompted full path.",
)
@click.option("--limit", default=500, show_default=True, help="Max rows to display.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--reports-config", default=None, help="Override reports config path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
def view_report(
    report_name, report, export, limit, pipeline_db, reports_config, sources_config
):
    """View the source table a report runs against.

    Shows ingested data with a _status column (flagged / ingested).

    \b
    Examples:
      vp view report
      vp view report daily_sales_validation
      vp view report --report daily_sales_validation --export csv
      vp view report --report daily_sales_validation --export custom
    """
    from proto_pipe.io.config import ReportConfig, SourceConfig

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    rep_cfg = config_path_or_override("reports_config", reports_config)
    src_cfg = config_path_or_override("sources_config", sources_config)

    config = ReportConfig(rep_cfg)
    name = _resolve_name(
        report_name, report, config.names(), "Which report would you like to view?"
    )
    if not name:
        click.echo("No reports configured. Run: vp new report")
        return

    existing = config.get(name)
    if not existing:
        click.echo(f"[error] No report named '{name}' found.")
        return

    table = existing.get("source", {}).get("table")
    if not table:
        click.echo(f"[error] Report '{name}' has no source table configured.")
        return

    src_config = SourceConfig(src_cfg)
    source = src_config.get_by_table(table)
    pk_col = source.get("primary_key") if source else None

    conn = duckdb.connect(p_db)
    try:
        df = query_table_with_status(conn, table, pk_col, limit)
        if df.empty:
            click.echo(f"Table '{table}' is empty. Run: vp ingest")
            return
        _show_or_export(
            df, f"report: {name} → {table} ({len(df)} rows)", export, pk_col
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp view deliverable
# ---------------------------------------------------------------------------

@view_cmd.command("deliverable")
@click.argument("deliverable_name", required=False)
@click.option("--deliverable", default=None, help="Deliverable name.")
@click.option("--export", default=None, type=click.Choice(["csv", "term"]))
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--deliverables-config", default=None)
def view_deliverable(
    deliverable_name, deliverable, export, pipeline_db, deliverables_config
):
    """Preview full deliverable output before export.

    Executes the deliverable SQL query against the pipeline DB. No limit
    is applied — this is the full output the carrier would receive.

    If the SQL contains unfilled placeholders (e.g. <from_date>), the
    preview will fail with a hint to edit the SQL file first.

    \b
    Examples:
      vp view deliverable
      vp view deliverable carrier_a
      vp view deliverable --deliverable carrier_a --export csv
    """
    from proto_pipe.io.config import DeliverableConfig

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    del_cfg = config_path_or_override("deliverables_config", deliverables_config)

    config = DeliverableConfig(del_cfg)
    name = _resolve_name(
        deliverable_name, deliverable, config.names(),
        "Which deliverable would you like to preview?"
    )
    if not name:
        click.echo("No deliverables configured. Run: vp new deliverable")
        return

    existing = config.get(name)
    if not existing:
        click.echo(f"[error] No deliverable named '{name}' found.")
        return

    sql_file = existing.get("sql_file")
    if not sql_file or not Path(sql_file).exists():
        click.echo(
            f"[error] No SQL file found for deliverable '{name}'.\n"
            f"  Expected: {sql_file or '(not set)'}\n"
            f"  Run: vp new sql   to scaffold one."
        )
        return

    sql = Path(sql_file).read_text().strip()
    if not sql:
        click.echo(f"[error] SQL file '{sql_file}' is empty.")
        return

    conn = duckdb.connect(p_db)
    try:
        try:
            df = conn.execute(sql).df()
        except Exception as exc:
            click.echo(
                f"[error] Could not execute deliverable SQL: {exc}\n"
                f"  If your SQL contains placeholders like '<from_date>', "
                f"replace them with actual values before previewing.\n"
                f"  SQL file: {sql_file}"
            )
            return

        if df.empty:
            click.echo(f"Query returned no rows for deliverable '{name}'.")
            return

        _show_or_export(df, f"deliverable: {name} ({len(df)} rows)", export)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp view source
# ---------------------------------------------------------------------------


@view_cmd.command("source")
@click.argument("table_name", required=False)
@click.option("--table", default=None, help="Source table name.")
@click.option(
    "--export",
    default=None,
    type=click.Choice(["csv", "custom"]),
    help="csv: auto-named file in output_dir. custom: prompted full path.",
)
@click.option("--limit", default=500, show_default=True, help="Max rows to display.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--sources-config", default=None, help="Override sources config path.")
def view_source(table_name, table, export, limit, pipeline_db, sources_config):
    """View ingested rows for a source table.

    Adds a _status column showing whether each row is flagged or clean.

    \b
    Examples:
      vp view source
      vp view source sales
      vp view source --table sales --export csv
      vp view source --table sales --export custom
    """
    from proto_pipe.io.config import SourceConfig

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    src_cfg = config_path_or_override("sources_config", sources_config)

    config = SourceConfig(src_cfg)
    source_tables = [s["target_table"] for s in config.all()]

    name = _resolve_name(
        table_name, table, source_tables, "Which source table would you like to view?"
    )
    if not name:
        click.echo("No sources configured. Run: vp new source")
        return

    source = config.get_by_table(name)
    if not source:
        click.echo(f"[error] No source found for table '{name}'")
        return

    pk_col = source.get("primary_key")

    conn = duckdb.connect(p_db)
    try:
        df = query_table_with_status(conn, name, pk_col, limit)
        if df.empty:
            click.echo(f"'{name}' is empty. Run: vp ingest")
            return
        _show_or_export(df, f"source: {name} ({len(df)} rows)", export, pk_col)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp view lineage
# ---------------------------------------------------------------------------

@view_cmd.command("lineage")
@click.option("--source",      default=None, help="Filter to the branch rooted at this source table.")
@click.option("--report",      default=None, help="Filter to this report and its upstream/downstream.")
@click.option("--deliverable", default=None, help="Trace backward from this deliverable to its roots.")
@click.option("--pipeline-db",         default=None, help="Override pipeline DB path.")
@click.option("--sources-config",      default=None, help="Override sources config path.")
@click.option("--reports-config",      default=None, help="Override reports config path.")
@click.option("--deliverables-config", default=None, help="Override deliverables config path.")
def view_lineage(
    source, report, deliverable,
    pipeline_db, sources_config, reports_config, deliverables_config,
):
    """Show the dependency graph from source files to deliverables.

    Reads config files for structure, enriches with last-run timestamps
    from the pipeline DB. Timestamps show when each node was last processed.

    Use filters to trace a specific branch:
      --source trace forward from a source table
      --report shows a report and everything upstream and downstream
      --deliverable trace backward from a deliverable to its roots

    No filter shows the full graph.

    \b
    Examples:
      vp view lineage
      vp view lineage --source sales
      vp view lineage --report sales_validation
      vp view lineage --deliverable monthly_pack
    """
    from proto_pipe.io.config import SourceConfig, ReportConfig, DeliverableConfig
    from proto_pipe.reports.runner import get_target_table
    from proto_pipe.pipelines.query import (
        build_lineage_graph, resolve_report_root_table, query_lineage_timestamps,
    )

    p_db = config_path_or_override("pipeline_db",         pipeline_db)
    src_cfg = config_path_or_override("sources_config",      sources_config)
    rep_cfg = config_path_or_override("reports_config",      reports_config)
    del_cfg = config_path_or_override("deliverables_config", deliverables_config)

    # ── Load configs ────────────────────────────────────────────────────────
    src_config = SourceConfig(src_cfg)
    rep_config = ReportConfig(rep_cfg)
    del_config = DeliverableConfig(del_cfg)

    all_sources = src_config.all()
    all_reports = rep_config.all()
    all_deliverables = del_config.all()

    # ── Load timestamps from pipeline DB ────────────────────────────────────
    last_ingested: dict[str, str] = {}
    last_validated: dict[str, str] = {}
    last_produced: dict[str, str] = {}

    try:
        conn = duckdb.connect(p_db)
        try:
            last_ingested, last_validated, last_produced = query_lineage_timestamps(conn)
        finally:
            conn.close()
    except Exception:
        pass  # DB may not exist yet — timestamps will all show "never"

    # ── Build graph ──────────────────────────────────────────────────────────
    graph = build_lineage_graph(all_sources, all_reports, get_target_table)

    # ── Apply filters ────────────────────────────────────────────────────────
    filter_count = sum([bool(source), bool(report), bool(deliverable)])
    if filter_count > 1:
        click.echo("[error] Only one of --source, --report, --deliverable may be used at a time.")
        return

    # Determine which source tables to display
    if source:
        if source not in graph.table_to_source:
            click.echo(f"[error] No source table named '{source}' found in sources_config.yaml")
            return
        display_sources = [graph.table_to_source[source]]
    elif report:
        if report not in graph.report_by_name:
            click.echo(f"[error] No report named '{report}' found in reports_config.yaml")
            return
        # Find the root source table of this report
        root_table = resolve_report_root_table(graph, report)
        display_sources = [graph.table_to_source[root_table]] if root_table in graph.table_to_source else []
    elif deliverable:
        del_entry = del_config.get(deliverable)
        if not del_entry:
            click.echo(f"[error] No deliverable named '{deliverable}' found in deliverables_config.yaml")
            return
        # Collect all source tables feeding this deliverable
        root_tables: set[str] = set()
        for rep_cfg_entry in del_entry.get("reports", []):
            rname = rep_cfg_entry.get("name", "")
            if rname in graph.report_by_name:
                rt = resolve_report_root_table(graph, rname)
                if rt:
                    root_tables.add(rt)
        display_sources = [graph.table_to_source[t] for t in root_tables if t in graph.table_to_source]
    else:
        display_sources = all_sources

    # ── Render ───────────────────────────────────────────────────────────────
    if not display_sources:
        click.echo("No sources to display. Run: vp new source")
        return

    # Map deliverable name → set of report names it includes
    deliverable_reports: dict[str, list[str]] = {
        d["name"]: [r.get("name", "") for r in d.get("reports", [])]
        for d in all_deliverables
    }
    # Invert: report name → list of deliverable names
    report_to_deliverables: dict[str, list[str]] = {}
    for dname, rnames in deliverable_reports.items():
        for rname in rnames:
            report_to_deliverables.setdefault(rname, []).append(dname)

    click.echo()

    def _render_report(rname: str, indent: int, filter_report: str | None, filter_deliverable: str | None) -> None:
        """Recursively render a report and its downstream dependents and deliverables."""
        # Apply report filter — only show this report and its chain
        if filter_report and rname != filter_report:
            # Still recurse in case the filter target is downstream
            for child in graph.report_dependents.get(rname, []):
                _render_report(child, indent, filter_report, filter_deliverable)
            return

        ts = last_validated.get(rname, "never")
        prefix = "  " * indent + "└── "
        click.echo(f"{prefix}{rname}  (last validated: {ts})")

        # Render downstream reports first
        for child in sorted(graph.report_dependents.get(rname, [])):
            _render_report(child, indent + 1, None, filter_deliverable)

        # Render deliverables that include this report
        for dname in sorted(report_to_deliverables.get(rname, [])):
            if filter_deliverable and dname != filter_deliverable:
                continue
            dts = last_produced.get(dname, "never")
            d_prefix = "  " * (indent + 1) + "└── "
            click.echo(f"{d_prefix}{dname}  (last produced: {dts})")

    for src in sorted(display_sources, key=lambda s: s["target_table"]):
        tbl = src["target_table"]
        patterns = ", ".join(src.get("patterns", []))
        ts = last_ingested.get(tbl, "never")
        click.echo(f"{patterns}  →  {tbl}  (last ingested: {ts})")

        # Find reports rooted directly in this source table (no upstream report)
        rooted_reports = [
            r["name"] for r in all_reports
            if r.get("source", {}).get("table") == tbl
            and graph.report_upstream.get(r["name"]) is None
        ]
        for rname in sorted(rooted_reports):
            _render_report(rname, indent=1, filter_report=report, filter_deliverable=deliverable)

        click.echo()


# ---------------------------------------------------------------------------
# vp view table
# ---------------------------------------------------------------------------
@view_cmd.command("table")
@click.argument("table_name", required=False)
@click.option("--table", default=None, help="Table name (alternative to argument).")
@click.option(
    "--export",
    default=None,
    type=click.Choice(["csv", "custom"]),
    help="csv: auto-named file in output_dir. custom: prompted full path.",
)
@click.option("--limit", default=500, show_default=True, help="Max rows to display.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def view_table(table_name, table, export, limit, pipeline_db):
    """View any pipeline table with rich display.

    \b
    Examples:
      vp view table
      vp view table sales
      vp view table ingest_state
      vp view table --table sales --export csv
      vp view table --table sales --export custom
    """
    from proto_pipe.io.config import SourceConfig, load_settings

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    name = table_name or table

    conn = duckdb.connect(p_db)
    try:
        all_tables = get_all_tables(conn)

        if not all_tables:
            click.echo("No tables found in the pipeline DB. Run: vp ingest")
            return

        if not name:
            user_tables = [t for t in all_tables if t not in PIPELINE_TABLES]
            infra_tables = [t for t in all_tables if t in PIPELINE_TABLES]

            choices = []
            if user_tables:
                choices.append(questionary.Separator("── Data Tables ──"))
                choices.extend(user_tables)
            if infra_tables:
                choices.append(questionary.Separator("── Pipeline Tables ──"))
                choices.extend(infra_tables)

            name = questionary.select(
                "Which table would you like to view?",
                choices=choices,
            ).ask()
            if not name:
                click.echo("Cancelled.")
                return

        if name not in all_tables:
            click.echo(f"[error] Table '{name}' not found.")
            click.echo(f"Available: {', '.join(all_tables)}")
            return

        df = conn.execute(f'SELECT * FROM "{name}" LIMIT {limit}').df()
        if df.empty:
            click.echo(f"'{name}' is empty.")
            return

        pk_col = None
        if name not in PIPELINE_TABLES:
            try:
                settings = load_settings()
                src_cfg = settings["paths"]["sources_config"]
                src_config = SourceConfig(src_cfg)
                source = src_config.get_by_table(name)
                if source:
                    pk_col = source.get("primary_key")
            except Exception:
                pass

        _show_or_export(df, f"{name} ({len(df)} rows)", export, pk_col)

    finally:
        conn.close()


def view_commands(cli: click.Group) -> None:
    cli.add_command(view_cmd)
