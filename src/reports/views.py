"""
Views module.
Creates and refreshes DuckDB views from views_config.yaml.

Views are plain SQL SELECT statements stored as named objects in pipeline.db.
They are not materialised — they re-run their SQL every time they are queried,
so they always reflect current table data.

Views can reference other views. Creation order in views_config.yaml matters:
a view that references another view must be listed after the one it depends on.

Usage:
    from validation_pipeline.views import create_views, refresh_views
"""

from pathlib import Path

import duckdb
import yaml  # type: ignore


# ---------------------------------------------------------------------------
# View creation
# ---------------------------------------------------------------------------

def _load_sql(sql_file: str) -> str:
    path = Path(sql_file)
    if not path.exists():
        raise FileNotFoundError(f"View SQL file not found: {sql_file}")
    sql = path.read_text().strip()
    if not sql:
        raise ValueError(f"View SQL file is empty: {sql_file}")
    return sql


def create_views(
    conn: duckdb.DuckDBPyConnection,
    views: list[dict],
    replace: bool = False,
) -> list[str]:
    """Create views in the order they appear in views_config.yaml.

    Args:
        conn: Open DuckDB connection to pipeline.db.
        views:   List of view dicts from views_config.yaml.
                 Each entry: { name: str, sql_file: str }
        replace: If True, DROP and recreate existing views (used by refresh-views).
                 If False, skip views that already exist (used by db-init).

    Returns list of view names that were created.
    """
    created = []

    for view in views:
        name = view["name"]
        sql_file = view["sql_file"]
        sql = _load_sql(sql_file)

        if replace:
            conn.execute(f'DROP VIEW IF EXISTS "{name}"')

        keyword = "CREATE OR REPLACE VIEW" if replace else "CREATE VIEW IF NOT EXISTS"
        conn.execute(f'{keyword} "{name}" AS {sql}')

        action = "refreshed" if replace else "created"
        print(f"  [ok]   View '{name}' {action}")
        created.append(name)

    return created


def refresh_views(
    conn: duckdb.DuckDBPyConnection,
    views: list[dict],
) -> list[str]:
    """Drop and recreate all views. Called by vp refresh-views and vp run-all.
    Preserves creation order so views that depend on other views work correctly.
    """
    return create_views(conn, views, replace=True)


# ---------------------------------------------------------------------------
# Config loader helper
# ---------------------------------------------------------------------------

def load_views_config(views_config_path: str) -> list[dict]:
    """Load views_config.yaml and return the list of view definitions.
    Returns an empty list if the file doesn't exist, so the pipeline
    degrades gracefully when no views are defined.
    """
    path = Path(views_config_path)
    if not path.exists():
        return []
    with open(path) as f:
        config = yaml.safe_load(f) or {}
    return config.get("views", [])
