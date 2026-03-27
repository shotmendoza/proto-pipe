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

def _load_sql(
        sql_file: str
) -> str:
    """Loads the content of a SQL file from the given path, ensuring the file exists
    and is not empty. If the file does not exist, a FileNotFoundError is raised.
    If the file exists but is empty, a ValueError is raised.

    :param sql_file: The path to the SQL file to be loaded.
    :type sql_file: str
    :return: The content of the SQL file as a string.
    :rtype: str
    :raises FileNotFoundError: If the provided SQL file does not exist.
    :raises ValueError: If the provided SQL file is empty.
    """
    path = Path(sql_file)
    if not path.exists():
        raise FileNotFoundError(f"View SQL file not found: {sql_file}")
    sql = path.read_text().strip()
    if not sql:
        raise ValueError(f"View SQL file is empty: {sql_file}")
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql


def create_views(
    conn: duckdb.DuckDBPyConnection,
    views: list[dict],
    replace: bool = False,
    skip_missing_tables: bool = False,
) -> list[str]:
    """Creates views in the specified DuckDB database connection based on the provided
    list of view definitions. Each view is defined by its name and the SQL file
    containing its query. Existing views can optionally be replaced, and the function
    also handles cases where tables referenced by views are missing.

    :param conn: A DuckDB database connection object used to execute the SQL commands.
                 Must be an instance of `duckdb.DuckDBPyConnection`.
    :param views: List of dictionaries describing the views to create. Each dictionary
                  must include the keys "name" (view name) and "sql_file" (path to the
                  SQL file defining the view's query).
    :param replace: Indicates whether to replace existing views if they already exist.
                    Defaults to False.
    :param skip_missing_tables: Determines whether to skip views that reference tables
                                that are missing. Defaults to False. Note: Function
                                implementation does not validate this behavior explicitly.
    :return: A list of names of the successfully created or refreshed views.
    :rtype: list of str
    """
    created = []

    for view in views:
        name = view["name"]
        sql_file = view["sql_file"]

        try:
            sql = _load_sql(sql_file)
        except (FileNotFoundError, ValueError) as exc:
            raise type(exc)(str(exc)) from exc

        if replace:
            conn.execute(f'DROP VIEW IF EXISTS "{name}"')

        keyword = "CREATE OR REPLACE VIEW" if replace else "CREATE VIEW IF NOT EXISTS"
        try:
            conn.execute(f'{keyword} "{name}" AS {sql}')
        except Exception as exc:
            err_str = str(exc).lower()
            if skip_missing_tables and (
                    "table" in err_str and ("does not exist" in err_str or "not found" in err_str)):
                print(f"[skip] View '{name}' — underlying table not yet ingested. ")
                print(f"Run vp refresh-views after first ingest.")
                continue

            # Re-raise with context so the user knows which view failed
            raise type(exc)(
                f"Failed to create view '{name}' from '{sql_file}':\n{exc}\n\n"
                f"If this view references another view, make sure that view "
                f"is listed first in views_config.yaml."
            ) from exc

        action = "refreshed" if replace else "created"
        print(f"[ok] View '{name}' {action}")
        created.append(name)

    return created


def refresh_views(
    conn: duckdb.DuckDBPyConnection,
    views: list[dict],
) -> list[str]:
    """Drop and recreate all views. Called by vp refresh-views and vp run-all.
    Preserves creation order so views that depend on other views work correctly.
    """
    return create_views(conn, views, replace=True, skip_missing_tables=False)


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
