"""Refresh commands — vp refresh source, vp refresh report, vp refresh views.

Cleans up stale flags for records that have already been resolved.

Use case: records failed ingest (type_conflict) and landed in source_block.
User fixed column types via vp edit column-type and re-ran vp ingest.
Records are now in source_pass but the old flags were never cleared.
vp refresh source removes only those stale flags.

Targeting: only records present in BOTH the flag table AND the pass table
are affected. The (table_name, pk_value) composite join ensures a PK from
one table never incorrectly matches another table's pass state.
"""

import click
import duckdb

from proto_pipe.io.config import config_path_or_override


# ---------------------------------------------------------------------------
# vp refresh group
# ---------------------------------------------------------------------------


@click.group("refresh", context_settings={"max_content_width": 120})
def refresh_cmd():
    """Clear stale flags for records that have already been resolved.

    \b
    Examples:
      vp refresh source
      vp refresh source sales
      vp refresh report
      vp refresh report daily_sales_validation
      vp refresh views
    """
    pass


# ---------------------------------------------------------------------------
# vp refresh source [table_name]
# ---------------------------------------------------------------------------


@refresh_cmd.command("source")
@click.argument("table_name", required=False, default=None)
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def refresh_source(table_name, yes, pipeline_db):
    """Clear stale source_block flags for resolved records.

    Targets only records present in BOTH source_block AND source_pass.
    Records that are only in source_block (still genuinely unresolved)
    are left untouched. source_pass, the source table, and the watermark
    are never modified.

    If TABLE_NAME is omitted, all tables are refreshed in one pass.

    \b
    Examples:
      vp refresh source
      vp refresh source sales
      vp refresh source sales --yes
    """
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        if table_name:
            count = conn.execute(
                """
                SELECT count(*) FROM source_block
                WHERE table_name = ?
                AND (table_name, pk_value) IN (
                    SELECT table_name, pk_value FROM source_pass
                )
                """,
                [table_name],
            ).fetchone()[0]
        else:
            count = conn.execute(
                """
                SELECT count(*) FROM source_block
                WHERE (table_name, pk_value) IN (
                    SELECT table_name, pk_value FROM source_pass
                )
                """
            ).fetchone()[0]

        scope = f"table '{table_name}'" if table_name else "all tables"

        if count == 0:
            click.echo(f"  No resolved flags found for {scope} — nothing to clear.")
            return

        if not yes:
            try:
                click.confirm(
                    f"  Clear {count} resolved flag(s) from source_block for {scope}?",
                    abort=True,
                )
            except click.Abort:
                click.echo("\n  Cancelled.")
                return

        if table_name:
            conn.execute(
                """
                DELETE FROM source_block
                WHERE table_name = ?
                AND (table_name, pk_value) IN (
                    SELECT table_name, pk_value FROM source_pass
                )
                """,
                [table_name],
            )
        else:
            conn.execute(
                """
                DELETE FROM source_block
                WHERE (table_name, pk_value) IN (
                    SELECT table_name, pk_value FROM source_pass
                )
                """
            )

        click.echo(
            f"[ok] {count} resolved flag(s) cleared from source_block for {scope}."
        )

    except Exception as e:
        click.echo(f"[error] {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp refresh report [report_name]
# ---------------------------------------------------------------------------


@refresh_cmd.command("report")
@click.argument("report_name", required=False, default=None)
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
def refresh_report(report_name, yes, pipeline_db):
    """Clear stale validation_block flags for resolved records.

    Targets only records present in BOTH validation_block AND
    validation_pass. Records only in validation_block (still genuinely
    unresolved) are left untouched. validation_pass and the report table
    are never modified.

    If REPORT_NAME is omitted, all reports are refreshed in one pass.

    \b
    Examples:
      vp refresh report
      vp refresh report daily_sales_validation
      vp refresh report daily_sales_validation --yes
    """
    p_db = config_path_or_override("pipeline_db", pipeline_db)
    conn = duckdb.connect(p_db)
    try:
        if report_name:
            count = conn.execute(
                """
                SELECT count(*) FROM validation_block
                WHERE report_name = ?
                AND (report_name, pk_value) IN (
                    SELECT report_name, pk_value FROM validation_pass
                )
                """,
                [report_name],
            ).fetchone()[0]
        else:
            count = conn.execute(
                """
                SELECT count(*) FROM validation_block
                WHERE (report_name, pk_value) IN (
                    SELECT report_name, pk_value FROM validation_pass
                )
                """
            ).fetchone()[0]

        scope = f"report '{report_name}'" if report_name else "all reports"

        if count == 0:
            click.echo(f"  No resolved flags found for {scope} — nothing to clear.")
            return

        if not yes:
            try:
                click.confirm(
                    f"  Clear {count} resolved flag(s) from validation_block"
                    f" for {scope}?",
                    abort=True,
                )
            except click.Abort:
                click.echo("\n  Cancelled.")
                return

        if report_name:
            conn.execute(
                """
                DELETE FROM validation_block
                WHERE report_name = ?
                AND (report_name, pk_value) IN (
                    SELECT report_name, pk_value FROM validation_pass
                )
                """,
                [report_name],
            )
        else:
            conn.execute(
                """
                DELETE FROM validation_block
                WHERE (report_name, pk_value) IN (
                    SELECT report_name, pk_value FROM validation_pass
                )
                """
            )

        click.echo(
            f"[ok] {count} resolved flag(s) cleared from validation_block for {scope}."
        )

    except Exception as e:
        click.echo(f"[error] {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# vp refresh views  (was: vp refresh-views)
# ---------------------------------------------------------------------------


@refresh_cmd.command("views")
@click.option("--pipeline-db", default=None, help="Override pipeline DB path.")
@click.option("--views-config", default=None, help="Override views config path.")
def refresh_views_cmd(pipeline_db, views_config):
    """Drop and recreate all views from views_config.yaml.

    Run this after editing a view SQL file. Views are also refreshed
    automatically during run-all before deliverables are produced.

    \b
    Example:
      vp refresh views
    """
    import duckdb

    from proto_pipe.reports.views import load_views_config, refresh_views

    p_db = config_path_or_override("pipeline_db", pipeline_db)
    v_cfg = config_path_or_override("views_config", views_config)

    views = load_views_config(v_cfg)
    if not views:
        click.echo(f"[skip] No views defined in {v_cfg}")
        return

    click.echo(f"\nRefreshing {len(views)} view(s) from: {v_cfg}")
    conn = duckdb.connect(p_db)
    try:
        refresh_views(conn, views)
        click.echo("\nDone.")
    except Exception as e:
        click.echo(f"[error] {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def refresh_commands(cli: click.Group) -> None:
    cli.add_command(refresh_cmd)
