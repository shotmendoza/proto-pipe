"""CLI prompts — SourceConfigPrompter, ReportConfigPrompter, DeliverableConfigPrompter.

Each prompter owns all questionary interactions for its resource type.
No DB connections, no business logic — pure UI.

Usage pattern:
    prompter = SourceConfigPrompter(sample_df=df, registry_hints=hints)
    if not prompter.run(existing_names, suggested_pattern):
        return  # cancelled
    config.add_or_update(prompter.source)
    write_registry_types(conn, prompter.source["name"], prompter.confirmed_types)
"""
from __future__ import annotations

import click
import questionary

from proto_pipe.constants import DUCKDB_TYPES, DATE_FORMATS


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


def _try_strptime(value: str, fmt: str) -> bool:
    """Return True if strptime(value, fmt) succeeds."""
    from datetime import datetime
    try:
        datetime.strptime(str(value), fmt)
        return True
    except (ValueError, TypeError):
        return False


# ---------------------------------------------------------------------------
# SourceConfigPrompter
# ---------------------------------------------------------------------------

class SourceConfigPrompter:
    """Collects user input for creating or editing a source configuration.

    Single-use: create a fresh instance per command invocation.
    After run() returns True, read results from self.source and self.confirmed_types.

    :param sample_df:       DataFrame for type inference and display, or None.
    :param registry_hints:  {column: {source_name: declared_type}} for conflict display.
    :param existing_source: Existing source dict when editing, None when creating.
    """
    def __init__(
        self,
        sample_df=None,
        registry_hints: dict | None = None,
        existing_source: dict | None = None,
    ) -> None:
        self._sample = sample_df
        self._registry_hints = registry_hints or {}
        self._existing = existing_source or {}
        # When no sample is available (table not ingested yet), fall back to
        # registry hint keys so selectors still work during edit.
        if sample_df is not None:
            self._file_cols = [c for c in sample_df.columns if not c.startswith("_")]
        elif self._registry_hints:
            self._file_cols = sorted(self._registry_hints.keys())
        else:
            self._file_cols = []
        # Output properties — only valid after run() returns True
        self.source: dict = {}
        self.confirmed_types: dict = {}

    # ---------------------------------------------------------------------------
    # run() — full source config flow
    # ---------------------------------------------------------------------------

    def run(self, existing_names: list[str], suggested_pattern: str) -> bool:
        """Run the full source configuration flow.

        Stores the completed source dict in self.source and confirmed
        column types in self.confirmed_types.

        Returns True on completion, False if the user cancels at any step.
        self.source and self.confirmed_types are only valid when True is returned.
        """
        self.source = {}
        self.confirmed_types = {}

        name = self.prompt_name(existing_names)
        if not name:
            return False

        patterns = self.prompt_pattern(suggested_pattern)
        if not patterns:
            return False

        table = self.prompt_target_table(default=name)
        if not table:
            return False

        primary_key = self.prompt_primary_key()
        if not primary_key:
            click.echo(
                "\n  [warn] No primary key defined — all rows will be appended"
                " and duplicates won't be detected."
            )

        on_duplicate = None
        timestamp_col = None
        if primary_key:
            on_duplicate = self.prompt_on_duplicate()
            if on_duplicate is None:
                return False
            timestamp_col = self.prompt_timestamp_col()

        self.confirmed_types = self.prompt_column_types()

        self.source = {"name": name, "patterns": patterns, "target_table": table}
        if timestamp_col:
            self.source["timestamp_col"] = timestamp_col
        if primary_key:
            self.source["primary_key"] = primary_key
            self.source["on_duplicate"] = on_duplicate

        return True

    # ---------------------------------------------------------------------------
    # Individual prompt methods
    # ---------------------------------------------------------------------------

    def prompt_name(self, existing_names: list[str]) -> str | None:
        """Prompt for source name. Returns None if cancelled."""
        default = self._existing.get("name", "")
        name = questionary.text(
            "Source name — a label for this data source"
            " (e.g. 'sales' for sales reports, 'inventory' for stock files):",
            default=default,
        ).ask()
        if not name:
            return None

        if name in existing_names and name != self._existing.get("name"):
            overwrite = questionary.confirm(
                f"Source '{name}' already exists. Edit it?"
            ).ask()
            if not overwrite:
                return None

        return name

    def prompt_pattern(self, suggested: str) -> list[str] | None:
        """Prompt for file pattern(s). Returns None if cancelled."""
        default = ", ".join(self._existing.get("patterns", [suggested]))
        pattern_input = questionary.text(
            "File pattern(s) — the naming convention used for these files, comma separated"
            " (e.g. sales_*.csv, Sales_*.xlsx).\n  Use * as a wildcard to match dates or"
            " version numbers in filenames:",
            default=default,
        ).ask()
        if not pattern_input:
            return None
        return [p.strip() for p in pattern_input.split(",")]

    def prompt_target_table(self, default: str) -> str | None:
        """Prompt for target table name. Returns None if cancelled."""
        current = self._existing.get("target_table", default)
        table = questionary.text(
            "Target table name — the name of the database table these files will be"
            " loaded into (press Enter to use source name):",
            default=current,
        ).ask()
        return table.strip() if table else None

    def prompt_primary_key(self) -> str | None:
        """Prompt for primary key column. Returns None if no PK defined."""
        current_pk = self._existing.get("primary_key")

        if self._file_cols:
            choices = self._file_cols + ["None — no primary key"]
            default = current_pk if current_pk in self._file_cols else "None — no primary key"
            pk_choice = questionary.select(
                "Primary key column — the column that uniquely identifies each row."
                " Select 'None' if not applicable:",
                choices=choices,
                default=default,
            ).ask()
            return None if pk_choice == "None — no primary key" else pk_choice
        else:
            pk_input = questionary.text(
                "Primary key column — the column that uniquely identifies each row"
                " (e.g. 'order_id', 'sku'). Leave blank if none:",
                default=current_pk or "",
            ).ask()
            return pk_input.strip() if pk_input else None

    def prompt_on_duplicate(self) -> str | None:
        """Prompt for duplicate row handling. Returns None if cancelled."""
        current = self._existing.get("on_duplicate", "flag")
        return questionary.select(
            "Duplicate row handling — what should happen when a new file contains"
            " a row whose primary key already exists?",
            choices=[
                questionary.Choice(
                    "flag   — flag conflicts for manual review (recommended)",
                    value="flag",
                ),
                questionary.Choice(
                    "upsert — replace existing row with incoming row",
                    value="upsert",
                ),
                questionary.Choice(
                    "append — insert all rows, allow duplicates",
                    value="append",
                ),
                questionary.Choice(
                    "skip   — keep existing row, ignore incoming",
                    value="skip",
                ),
            ],
            default=current,
        ).ask()

    def prompt_timestamp_col(self) -> str | None:
        """Prompt for timestamp column. Returns None if using _ingested_at."""
        current = self._existing.get("timestamp_col")
        sentinel = "None — use _ingested_at"

        if self._file_cols:
            default = current if current in self._file_cols else sentinel
            ts_choice = questionary.select(
                "Timestamp column — the column that tracks when each row was created or"
                " updated.\n  Used for incremental runs. Select 'None' to use"
                " _ingested_at (the pipeline ingestion time):",
                choices=self._file_cols + [sentinel],
                default=default,
            ).ask()
            return None if ts_choice == sentinel else ts_choice
        else:
            ts_input = questionary.text(
                "Timestamp column — the column that tracks when each row was created or"
                " updated (e.g. 'updated_at').\n  Leave blank to use _ingested_at:",
                default=current or "",
            ).ask()
            return ts_input.strip() if ts_input else None

    def prompt_column_types(self) -> dict[str, str]:
        """Show inferred type summary and let the user confirm or override.

        Returns {column_name: declared_type} for all file columns.
        Returns {} if no columns are available (no sample and no registry entries).

        When sample_df is None, sample values are omitted from the display
        but column type selection still works via registry data.
        """
        if not self._file_cols:
            return {}

        # Infer types from sample where available
        inferred_types: dict[str, str] = {}
        if self._sample is not None:
            inferred_types = {
                col: _infer_duckdb_type(self._sample[col])
                for col in self._file_cols
                if col in self._sample.columns
            }

        working_types: dict[str, str] = {}
        for col in self._file_cols:
            hints = self._registry_hints.get(col, {})
            unique_types = set(hints.values())
            if len(unique_types) == 1:
                working_types[col] = next(iter(unique_types))
            elif col in inferred_types:
                working_types[col] = inferred_types[col]
            else:
                working_types[col] = "VARCHAR"

        click.echo("\n  Column types:")
        for col in self._file_cols:
            hints = self._registry_hints.get(col, {})
            unique_types = set(hints.values())
            dtype = working_types[col]

            if len(unique_types) == 0:
                note = ""
            elif len(unique_types) == 1:
                source_names = ", ".join(f"'{s}'" for s in hints.keys())
                note = f"  (registered in {source_names})"
            else:
                conflict_parts = ", ".join(f"{t} in '{s}'" for s, t in hints.items())
                note = f"  ⚠ conflict: {conflict_parts}"

            if self._sample is not None and col in self._sample.columns:
                non_null = self._sample[col].dropna()
                sample_str = (
                    f" sample: {non_null.iloc[0]}"
                    if not non_null.empty
                    else " sample: —"
                )
            else:
                sample_str = ""

            click.echo(f"    {col:<28} {dtype:<14}{sample_str}{note}")

        looks_right = questionary.confirm(
            "\n  Do these look right?", default=True
        ).ask()
        if looks_right is None:
            return {}

        if not looks_right:
            to_fix = questionary.checkbox(
                "Select the columns to change:",
                choices=[
                    questionary.Choice(
                        f"{col:<28} {working_types[col]:<14}"
                        + (
                            f" sample: {self._sample[col].dropna().iloc[0]}"
                            if self._sample is not None
                            and col in self._sample.columns
                            and not self._sample[col].dropna().empty
                            else ""
                        ),
                        value=col,
                    )
                    for col in self._file_cols
                ],
            ).ask()
            if to_fix is None:
                return {}

            for col in to_fix:
                current = working_types[col]
                hints = self._registry_hints.get(col, {})
                unique_types = set(hints.values())

                if len(unique_types) > 1:
                    conflict_parts = ", ".join(
                        f"{t} in '{s}'" for s, t in hints.items()
                    )
                    click.echo(
                        f"  ⚠ '{col}' has conflicting registry types: {conflict_parts}"
                    )

                chosen = questionary.select(
                    f"  '{col}' (currently {current}):",
                    choices=[
                        questionary.Choice(
                            f"{t}{' (current)' if t == current else ''}", value=t
                        )
                        for t in DUCKDB_TYPES
                    ],
                    default=current,
                ).ask()
                if chosen is None:
                    return {}
                working_types[col] = chosen

                if working_types[col] in ("DATE", "TIMESTAMPTZ"):
                    fmt = self.prompt_date_format(col)
                    if fmt:
                        working_types[col] = f"{working_types[col]}|{fmt}"

        return working_types

    def prompt_date_format(self, col: str) -> str | None:
        """Prompt for date format string when a date type is selected."""
        sample_val = None
        if self._sample is not None and col in self._sample.columns:
            non_null = self._sample[col].dropna()
            if not non_null.empty:
                sample_val = str(non_null.iloc[0])

        click.echo(f"\n  Date format for '{col}':")
        if sample_val:
            click.echo(f"  Sample value: {sample_val}\n")

        choices = []
        for fmt, example in DATE_FORMATS:
            matches = _try_strptime(sample_val, fmt) if sample_val else False
            label = f"{fmt:<16} e.g. {example}"
            if matches:
                label += "  ✓ matches sample"
            choices.append(questionary.Choice(label, value=fmt))
        choices.append(questionary.Choice("Enter custom format", value="__custom__"))
        choices.append(questionary.Choice("Skip — store as VARCHAR instead", value="__skip__"))

        chosen = questionary.select(f"  Select format for '{col}':", choices=choices).ask()

        if chosen is None or chosen == "__skip__":
            return None
        if chosen == "__custom__":
            custom = questionary.text("Enter strptime format string (e.g. %Y-%m-%d):").ask()
            return custom.strip() if custom else None
        return chosen

    @staticmethod
    def prompt_file_group(
        file_groups: "dict[str, list[str]]",
    ) -> "tuple[str | None, str | None]":
        """Show a pattern-grouped file picker.

        Each choice represents one suggested pattern and shows how many files
        it would cover. The user picks a pattern rather than an individual file.

        Returns (selected_file, suggested_pattern) where:
          - selected_file is the first file from the chosen group (used for
            column sampling). None if the user chose to define manually.
          - suggested_pattern is the chosen pattern string, pre-filled in the
            subsequent prompt_pattern step. None if the user cancelled (ESC).
        """
        choices = [
            questionary.Choice(
                title=f"{pattern}  ({len(files)} file{'s' if len(files) != 1 else ''})",
                value=pattern,
            )
            for pattern, files in sorted(
                file_groups.items(), key=lambda x: (-len(x[1]), x[0])
            )
        ] + ["None of these — define manually"]

        selected = questionary.select(
            "Which file pattern are you configuring a source for?",
            choices=choices,
        ).ask()

        if selected is None:
            return None, None  # ESC — cancelled

        if selected == "None of these — define manually":
            return None, "*.csv"

        matched_files = file_groups[selected]
        return matched_files[0], selected


# ---------------------------------------------------------------------------
# ReportConfigPrompter
# ---------------------------------------------------------------------------

class ReportConfigPrompter:
    """Collects user input for creating or editing a report configuration.

    Single-use: create a fresh instance per command invocation.
    After run() returns True, read results from self.name, self.table,
    self.check_entries, and self.alias_map.

    :param check_registry:   Populated CheckRegistry instance.
    :param p_db:             Path to the pipeline DB.
    :param multi_select:     Whether multi-select params are enabled.
    :param existing_report:  Existing report dict when editing, None when creating.
    """

    def __init__(
        self,
        check_registry,
        p_db: str,
        multi_select: bool = True,
        existing_report: dict | None = None,
    ) -> None:
        self._registry = check_registry
        self._p_db = p_db
        self._multi_select = multi_select
        self._existing = existing_report or {}
        # Output properties — only valid after run() returns True
        self.name: str = ""
        self.table: str = ""
        self.check_entries: list[dict] = []
        self.alias_map: list[dict] = []

    # ---------------------------------------------------------------------------
    # run() — full report config flow with ESC-to-go-back step machine
    # ---------------------------------------------------------------------------

    def run(
        self,
        available_tables: list[str],
        existing_names: list[str],
        conn,
    ) -> bool:
        """Run the full report configuration flow.

        Stores results in self.name, self.table, self.check_entries, self.alias_map.
        Returns True on completion, False if cancelled.
        """
        from proto_pipe.cli.scaffold import _get_table_columns

        self.name = ""
        self.table = ""
        self.check_entries = []
        self.alias_map = list(self._existing.get("alias_map", []))

        STEP_TABLE, STEP_NAME, STEP_CHECKS, STEP_PARAMS, STEP_DONE = 0, 1, 2, 3, 4
        state: dict = {"alias_map": self.alias_map}
        step = STEP_TABLE

        existing_check_names = [c["name"] for c in self._existing.get("checks", [])]

        while step < STEP_DONE:
            if step == STEP_TABLE:
                table = self.prompt_table(available_tables)
                if table is None:
                    return False
                state["table"] = table
                step = STEP_NAME

            elif step == STEP_NAME:
                name = self.prompt_name(
                    existing_names,
                    default=f"{state['table']}_validation",
                )
                if name is None:
                    step = STEP_TABLE
                    continue
                state["name"] = name
                step = STEP_CHECKS

            elif step == STEP_CHECKS:
                table_cols = sorted(_get_table_columns(self._p_db, state["table"]))
                selected, go_back = self.prompt_checks(
                    table_cols=table_cols,
                    alias_map=state.get("alias_map", []),
                    preselected=existing_check_names,
                )
                if go_back:
                    step = STEP_NAME
                    continue
                if not selected:
                    click.echo("  Please select at least one check.")
                    continue
                state["selected_checks"] = selected
                step = STEP_PARAMS

            elif step == STEP_PARAMS:
                check_entries, alias_map_entries, go_back = self.prompt_params(
                    selected_checks=state["selected_checks"],
                    table=state["table"],
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

        self.name = state["name"]
        self.table = state["table"]
        self.check_entries = state["check_entries"]
        self.alias_map = state.get("alias_map", [])
        return True

    # ---------------------------------------------------------------------------
    # Individual prompt methods
    # ---------------------------------------------------------------------------

    def prompt_table(self, available_tables: list[str]) -> str | None:
        """Prompt for source table selection. Returns None if cancelled."""
        default = self._existing.get("source", {}).get("table")
        return questionary.select(
            "Which table should this report run against?",
            choices=available_tables,
            default=default if default in available_tables else None,
        ).ask()

    def prompt_name(self, existing_names: list[str], default: str) -> str | None:
        """Prompt for report name. Returns None if ESC pressed."""
        current = self._existing.get("name", default)
        name = questionary.text("Report name:", default=current).ask()
        if name is None:
            return None

        if name in existing_names and name != self._existing.get("name"):
            overwrite = questionary.confirm(
                f"Report '{name}' already exists. Edit it?"
            ).ask()
            if not overwrite:
                return None

        return name

    def prompt_checks(
        self,
        table_cols: list[str],
        alias_map: list[dict],
        preselected: list[str] | None = None,
    ) -> tuple[list[str] | None, bool]:
        """Show check summary and checkbox. Returns (selected, go_back)."""
        from proto_pipe.cli.scaffold import (
            _get_original_func,
            _get_check_first_sentence,
            _build_check_param_lines,
        )

        available_checks = self._registry.available()
        preselected = preselected or []

        alias_param_to_cols: dict[str, list[str]] = {}
        for entry in alias_map:
            alias_param_to_cols.setdefault(entry["param"], []).append(entry["column"])

        click.echo("\n  Available checks:\n")
        choices = []
        for check_name in available_checks:
            original = _get_original_func(check_name, self._registry)
            first_sentence = _get_check_first_sentence(original) if original else ""
            param_lines = _build_check_param_lines(
                check_name, self._registry, table_cols, alias_param_to_cols
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
                    checked=check_name in preselected,
                )
            )

        selected = questionary.checkbox(
            "Select checks to run on this report:",
            choices=choices,
        ).ask()

        if selected is None:
            return None, True
        return selected, False

    def prompt_params(
        self,
        selected_checks: list[str],
        table: str,
        conn,
        report_name: str,
        existing_alias_map: list[dict] | None = None,
    ) -> tuple[list[dict], list[dict], bool]:
        """Fill params for selected checks. Delegates to _fill_params."""
        from proto_pipe.cli.scaffold import _fill_params

        return _fill_params(
            selected_checks=selected_checks,
            table=table,
            p_db=self._p_db,
            check_registry=self._registry,
            multi_select=self._multi_select,
            conn=conn,
            report_name=report_name,
            existing_alias_map=existing_alias_map,
        )


# ---------------------------------------------------------------------------
# DeliverableConfigPrompter
# ---------------------------------------------------------------------------

class DeliverableConfigPrompter:
    """Collects user input for creating or editing a deliverable configuration.

    Single-use: create a fresh instance per command invocation.
    After run() returns True, read the completed config from self.deliverable.

    :param rep_config:            Loaded reports config dict.
    :param src_config:            Loaded sources config dict.
    :param sql_dir:               Path to the SQL directory.
    :param existing_deliverable:  Existing deliverable dict when editing, None when creating.
    """

    def __init__(
        self,
        rep_config: dict,
        src_config: dict,
        sql_dir: str,
        existing_deliverable: dict | None = None,
    ) -> None:
        self._rep_config = rep_config
        self._src_config = src_config
        self._sql_dir = sql_dir
        self._existing = existing_deliverable or {}
        # Output property — only valid after run() returns True
        self.deliverable: dict = {}

    # ---------------------------------------------------------------------------
    # run() — full deliverable config flow
    # ---------------------------------------------------------------------------

    def run(self, existing_names: list[str], available_reports: list[str]) -> bool:
        """Run the full deliverable configuration flow.

        Stores the completed deliverable dict in self.deliverable.
        Returns True on completion, False if cancelled.
        """
        self.deliverable = {}

        name = self.prompt_name(existing_names)
        if not name:
            return False

        fmt = self.prompt_format()
        if not fmt:
            return False

        filename_template = self.prompt_filename_template(name, fmt)
        if not filename_template:
            return False

        selected_reports = self.prompt_reports(available_reports)
        if not selected_reports:
            return False

        if fmt == "csv" and len(selected_reports) > 1:
            click.echo(
                "\n  [warn] Multiple reports selected for a CSV deliverable —"
                " you'll need to join them in your SQL file."
            )

        sql_file = self.prompt_sql(name, selected_reports)
        report_entries = self.prompt_report_entries(selected_reports, fmt)

        self.deliverable = {
            "name": name,
            "format": fmt,
            "filename_template": filename_template,
            "reports": report_entries,
        }
        if sql_file:
            self.deliverable["sql_file"] = sql_file

        return True

    # ---------------------------------------------------------------------------
    # Individual prompt methods
    # ---------------------------------------------------------------------------

    def prompt_name(self, existing_names: list[str]) -> str | None:
        """Prompt for deliverable name. Returns None if cancelled."""
        current = self._existing.get("name", "")
        name = questionary.text(
            "Deliverable name (e.g. carrier_a):", default=current
        ).ask()
        if not name:
            return None

        if name in existing_names and name != self._existing.get("name"):
            overwrite = questionary.confirm(
                f"Deliverable '{name}' already exists. Edit it?"
            ).ask()
            if not overwrite:
                return None

        return name

    def prompt_format(self) -> str | None:
        """Prompt for output format. Returns None if cancelled."""
        current = self._existing.get("format", "xlsx")
        return questionary.select(
            "Output format:",
            choices=[
                questionary.Choice(
                    "xlsx — Excel file (supports multiple tabs)", value="xlsx"
                ),
                questionary.Choice("csv  — CSV file (single output)", value="csv"),
            ],
            default=current,
        ).ask()

    def prompt_filename_template(self, name: str, fmt: str) -> str | None:
        """Prompt for filename template. Returns None if cancelled."""
        default = self._existing.get("filename_template", f"{name}_{{date}}.{fmt}")
        result = questionary.text("Filename template:", default=default).ask()
        return result or None

    def prompt_reports(self, available_reports: list[str]) -> list[str] | None:
        """Prompt for report selection. Returns None if cancelled."""
        current = [r["name"] for r in self._existing.get("reports", [])]
        selected = questionary.checkbox(
            "Select reports to include in this deliverable:",
            choices=[
                questionary.Choice(r, value=r, checked=r in current)
                for r in available_reports
            ],
        ).ask()
        return selected if selected else None

    def prompt_sql(self, name: str, selected_reports: list[str]) -> str | None:
        """Prompt for SQL transformation file. Returns path or None."""
        from pathlib import Path
        from proto_pipe.cli.scaffold import _build_rich_sql_scaffold

        current_sql = self._existing.get("sql_file")
        use_sql = questionary.confirm(
            "Does this deliverable need a custom SQL transformation?",
            default=bool(current_sql) or True,
        ).ask()

        if not use_sql:
            return None

        sql_path = Path(self._sql_dir) / f"{name}.sql"
        sql_path.parent.mkdir(parents=True, exist_ok=True)

        if sql_path.exists():
            click.echo(f"  [skip] {sql_path} already exists — not overwriting")
        else:
            scaffold = _build_rich_sql_scaffold(
                name, selected_reports, self._rep_config, self._src_config
            )
            sql_path.write_text(scaffold)
            click.echo(f"  [ok]   {sql_path}")

        return str(sql_path)

    def prompt_report_entries(
        self, selected_reports: list[str], fmt: str
    ) -> list[dict]:
        """Prompt for per-report sheet names and date filters."""
        fill_details = questionary.confirm(
            "Would you like to fill in sheet names and date filters now?",
            default=False,
        ).ask()

        report_entries = []
        for report_name in selected_reports:
            entry = {"name": report_name}

            if fmt == "xlsx":
                if fill_details:
                    sheet = questionary.text(
                        f"Sheet name for '{report_name}':", default=report_name
                    ).ask()
                    entry["sheet"] = sheet or report_name
                else:
                    entry["sheet"] = report_name

            if fill_details:
                add_filter = questionary.confirm(
                    f"Add a date filter for '{report_name}'?", default=False
                ).ask()
                if add_filter:
                    date_col = questionary.text(
                        "Date column to filter on:", default="_ingested_at"
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

        return report_entries
