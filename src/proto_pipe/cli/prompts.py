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


def prompt_custom_export_path() -> "Path | None":
    """Prompt for a custom CSV export path with a pipeline warning.

    Shows a warning that custom paths are not picked up by vp ingest or
    vp flagged retry, then asks for a full output path from the user.

    Returns a resolved Path, or None if the user cancels.
    """
    from pathlib import Path

    click.echo(
        "\n  [warn] Custom paths won't be picked up by vp ingest or vp flagged retry. "
        "Use csv to export to output_dir so the pipeline can find the file automatically."
    )
    raw = questionary.text(
        "Full export path (including filename, e.g. /tmp/my_export.csv):"
    ).ask()
    if not raw:
        return None
    return Path(raw).expanduser().resolve()


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
# Helpers
# ---------------------------------------------------------------------------


def _safe_sample_label(sample, col: str) -> str:
    """Return a sanitised " sample: <value>" label for questionary choice display.

    Questionary uses Rich internally, so brackets like [INFO] in a value
    would be interpreted as markup tags and either hidden or cause render
    errors. We escape brackets, strip newlines, and truncate to keep
    choice rows readable on any terminal width.

    Returns empty string when no sample is available for the column.
    """
    if sample is None or col not in sample.columns:
        return ""
    non_null = sample[col].dropna()
    if non_null.empty:
        return ""
    raw = str(non_null.iloc[0])
    # Strip newlines and control characters
    raw = " ".join(raw.splitlines()).strip()
    # Escape Rich markup brackets so questionary renders them literally
    raw = raw.replace("[", "\\[").replace("]", "\\]")
    # Truncate to keep the line readable
    if len(raw) > 30:
        raw = raw[:27] + "..."
    return f" sample: {raw}"


def prompt_delete_impact(
        rows: list[tuple[str, int, str]],
        yes: bool = False,
) -> bool:
    """Display an impact summary and prompt for confirmation before a delete.

    Shows what will be removed and asks the user to confirm. When yes=True,
    skips both the summary and the prompt and returns True immediately
    (scripting path).

    :param rows: List of (label, count, unit) tuples, e.g.:
                 [("table 'sales'", 1234, "rows"),
                  ("ingest_state",  15,   "entries"),
                  ("source_block",  3,    "open flags"),
                  ("source_pass",   1234, "entries")]
    :param yes: If True, skip summary and prompt — return True immediately.
    :returns: True if the user confirmed or yes=True, False if cancelled.
    """
    if yes:
        return True

    click.echo("\n  This will remove:")
    for label, count, unit in rows:
        click.echo(f"    {label:<28} {count:,} {unit}")
    click.echo()

    try:
        click.confirm("  Continue?", abort=True)
        return True
    except click.Abort:
        click.echo("\n  Cancelled.")
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
            existing_sources_lookup: dict[str, dict] | None = None,
    ) -> None:
        self._sample = sample_df
        self._registry_hints = registry_hints or {}
        self._existing = existing_source or {}
        self._existing_lookup = existing_sources_lookup or {}
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
                f"Source '{name}' already exists. Edit it? "
                f"(patterns, PK, and other settings will be pre-filled)"
            ).ask()
            if not overwrite:
                return None
            # Load the existing source so all subsequent prompts pre-fill correctly.
            # prompt_pattern() will append the new suggested pattern to the existing list.
            if name in self._existing_lookup:
                self._existing = self._existing_lookup[name]

        return name

    def prompt_pattern(self, suggested: str) -> list[str] | None:
        """Prompt for file pattern(s). Returns None if cancelled.

        When editing an existing source, pre-fills with existing patterns and
        appends the new suggested pattern (if not already present) so the user
        confirms the combined list in one step.
        """
        existing_patterns = self._existing.get("patterns", [])
        if existing_patterns:
            # Append the new suggested pattern if it isn't already covered.
            if suggested and suggested not in existing_patterns:
                default_list = existing_patterns + [suggested]
            else:
                default_list = list(existing_patterns)
        else:
            default_list = [suggested] if suggested else []

        default = ", ".join(default_list)
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

            sample_str = _safe_sample_label(self._sample, col) or " sample: —"

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
                        + _safe_sample_label(self._sample, col),
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


def _make_col_choices(
        cols: list[str],
        registry_types: dict[str, str],
        precheck: "set[str] | None" = None,
) -> "list[questionary.Choice]":
    """Build questionary.Choice objects for column selection prompts.

    Each choice displays 'column_name (TYPE)' as the title and returns the
    column name as the value. Columns in precheck are pre-selected (used for
    checkbox prompts). Auto-populate fires only on exact name match — the
    caller sets precheck based on param_name == col, no fuzzy matching.
    """
    precheck = precheck or set()
    return [
        questionary.Choice(
            title=f"{col} ({registry_types[col]})" if col in registry_types else col,
            value=col,
            checked=col in precheck,
        )
        for col in cols
    ]


# Sentinel for the free-text escape hatch in column-picker prompts.
# When selected, the user types a value directly → filled_params (broadcast constant)
# rather than alias_map (column-backed per-row). Never stored in alias_map.
_DIRECT_ENTRY = "\u270e Enter value directly"


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
        from proto_pipe.cli.scaffold import get_table_columns

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
                from proto_pipe.io.db import get_registry_types
                table_cols = sorted(get_table_columns(self._p_db, state["table"]))
                registry_types = get_registry_types(conn, columns=table_cols)
                selected, go_back = self.prompt_checks(
                    table_cols=table_cols,
                    alias_map=state.get("alias_map", []),
                    registry_types=registry_types,
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

    def prompt_table(self, available_tables) -> str | None:
        """Prompt for source table selection. Returns None if cancelled.

        Accepts list[str] (plain table names) or list[tuple[str, int]]
        (table_name, report_count) from get_all_source_tables. When tuples
        are provided, annotated questionary.Choice objects are built here —
        keeping all questionary formatting inside prompts.py (CLAUDE.md rule).
        The value of each choice is always the raw table name string.
        """
        default = self._existing.get("source", {}).get("table")
        if available_tables and isinstance(available_tables[0], tuple):
            choices = [
                questionary.Choice(
                    title=f"{name}  ({count} report{'s' if count != 1 else ''})"
                    if count > 0 else name,
                    value=name,
                )
                for name, count in available_tables
            ]
            default_val = default if any(
                c.value == default for c in choices
            ) else None
        else:
            choices = list(available_tables)
            default_val = default if default in choices else None
        return questionary.select(
            "Which table should this report run against?",
            choices=choices,
            default=default_val,
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
            registry_types: dict[str, str],
            preselected: list[str] | None = None,
    ) -> tuple[list[str] | None, bool]:
        """Show available columns, then two sequential prompts — transforms then checks.

        Columns are printed once before any prompt so the user can verify
        type compatibility before selecting functions. Each kind is shown in
        its own checkbox. If a kind has no registered functions its prompt
        is skipped silently — no empty checkbox is shown.

        Returns (selected_names, go_back). go_back is True when the user
        pressed ESC on either prompt. selected_names is the merged list of
        transform + check selections in that order.
        """
        # -- Column display --------------------------------------------------
        if registry_types:
            click.echo("\n  Available columns:")
            for col, dtype in registry_types.items():
                click.echo(f"    {col} ({dtype})")
        else:
            click.echo(
                "\n  [warn] No column types registered for this source"
                " \u2014 run `vp new source` first"
            )

        # -- Kind split (consuming existing public methods, not metadata) -----
        transforms = self._registry.transforms_only()
        checks = self._registry.checks_only()

        alias_param_to_cols: dict[str, list[str]] = {}
        for entry in alias_map:
            alias_param_to_cols.setdefault(entry["param"], []).append(entry["column"])

        preselected = preselected or []
        selected: list[str] = []

        # -- Transforms prompt (skipped if none registered) ------------------
        if transforms:
            transform_selected, go_back = self._prompt_kind_group(
                names=transforms,
                label="Select transforms to apply:",
                table_cols=table_cols,
                alias_param_to_cols=alias_param_to_cols,
                preselected=preselected,
            )
            if go_back:
                return None, True
            selected.extend(transform_selected or [])

        # -- Checks prompt (skipped if none registered) ----------------------
        if checks:
            check_selected, go_back = self._prompt_kind_group(
                names=checks,
                label="Select validation checks:",
                table_cols=table_cols,
                alias_param_to_cols=alias_param_to_cols,
                preselected=preselected,
            )
            if go_back:
                return None, True
            selected.extend(check_selected or [])

        return selected, False

    def _prompt_kind_group(
            self,
            names: list[str],
            label: str,
            table_cols: list[str],
            alias_param_to_cols: dict[str, list[str]],
            preselected: list[str],
    ) -> tuple[list[str] | None, bool]:
        """Render metadata summary and checkbox for one kind group.

        Prints name, first docstring sentence, and param lines for each
        function before showing the checkbox. Returns (selected_names, go_back).
        go_back is True when the user pressed ESC (questionary returns None).
        """
        from proto_pipe.cli.scaffold import (
            get_original_func,
            get_check_first_sentence,
            build_check_param_lines,
        )

        click.echo()
        choices = []
        for check_name in names:
            original = get_original_func(check_name, self._registry)
            first_sentence = get_check_first_sentence(original) if original else ""
            param_lines = build_check_param_lines(
                check_name, self._registry, table_cols, alias_param_to_cols
            )

            click.echo(f"  {check_name}")
            if first_sentence:
                click.echo(f"    {first_sentence}")
            for line in param_lines:
                click.echo(line)
            click.echo()

            if first_sentence:
                short = (
                    first_sentence[:60] + "…"
                    if len(first_sentence) > 60
                    else first_sentence
                )
                title = f"{check_name:<32} {short}"
            else:
                title = check_name

            choices.append(
                questionary.Choice(
                    title=title,
                    value=check_name,
                    checked=check_name in preselected,
                )
            )

        selected = questionary.checkbox(label, choices=choices).ask()
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
        """Fill params for selected checks. Delegates to self._fill_params."""
        return self._fill_params(
            selected_checks=selected_checks,
            table=table,
            conn=conn,
            report_name=report_name,
            existing_alias_map=existing_alias_map,
        )

    def _fill_params(
            self,
            selected_checks: list[str],
            table: str,
            conn: "duckdb.DuckDBPyConnection",
            report_name: str,
            existing_alias_map: list[dict] | None = None,
    ) -> tuple[list[dict], list[dict], bool]:
        """Fill params for each selected check, building alias_map entries for column params.

        Column params → alias_map (not in params dict). Choices are displayed
        as 'column_name (TYPE)' sourced from column_type_registry. Auto-populate
        fires when param_name exactly matches a column name.
        Scalar params → params dict.
        DataFrame params → skipped (auto-filled at runtime).
        check_col (kind=check + returns DataFrame) → params dict.
        overwrite_cols (kind=transform + returns DataFrame) → params dict.

        Returns (check_entries, alias_map_entries, go_back).
        """
        import inspect
        from proto_pipe.cli.scaffold import (
            get_table_columns,
            get_original_func,
            get_check_params,
            get_param_suggestions,
            get_column_param_history,
            record_param_history,
            is_list_annotation,
        )
        from proto_pipe.io.db import get_registry_types

        table_cols = sorted(get_table_columns(self._p_db, table))
        registry_types = get_registry_types(conn, columns=table_cols)
        existing_alias_map = existing_alias_map or []

        alias_param_to_cols: dict[str, list[str]] = {}
        for entry in existing_alias_map:
            alias_param_to_cols.setdefault(entry["param"], []).append(entry["column"])

        accumulated_alias: list[dict] = list(existing_alias_map)

        checks_with_params = {
            c: get_check_params(c, self._registry)
            for c in selected_checks
            if get_check_params(c, self._registry)
        }

        # Also include checks that have no promptable params but need df-return prompts
        df_return_checks = set()
        for check_name in selected_checks:
            original = get_original_func(check_name, self._registry)
            if original:
                from proto_pipe.checks.registry import CheckParamInspector
                inspector = CheckParamInspector(original)
                if inspector.has_dataframe_input() and inspector.returns_dataframe():
                    df_return_checks.add(check_name)

        checks_needing_prompts = set(checks_with_params.keys()) | df_return_checks

        if not checks_needing_prompts:
            return [{"name": c} for c in selected_checks], accumulated_alias, False

        check_entries = []

        for check_name in selected_checks:
            params = checks_with_params.get(check_name, {})
            original = get_original_func(check_name, self._registry)
            from proto_pipe.checks.registry import CheckParamInspector
            inspector = CheckParamInspector(original) if original else None

            has_df_return = check_name in df_return_checks
            kind = self._registry.get_kind(check_name)

            if not params and not has_df_return:
                check_entries.append({"name": check_name})
                continue

            click.echo(f"\nParameters for '{check_name}':")

            eligible = (
                self._multi_select
                and inspector is not None
                and (
                    inspector.is_multiselect_eligible()  # checks: bool-Series return
                    or (                                  # transforms: col-backed params
                        kind == "transform"
                        and (
                            len(inspector.column_params()) > 0
                            or len(inspector.column_backed_scalar_params()) > 0
                        )
                    )
                )
            )
            col_params = inspector.column_params() if inspector else []
            # pd.Series params: column picker only, no escape hatch (unchanged behaviour)
            series_param_set = set(inspector.series_params()) if inspector else set()
            # int/float params eligible for column-backing (empty when pd.Series present)
            col_backed_scalars = set(inspector.column_backed_scalar_params()) if inspector else set()
            # All params that get column picker: Series + str/unannotated + eligible int/float
            all_col_picker_params = set(col_params) | col_backed_scalars
            sig = inspect.signature(inspector.func) if inspector else None

            filled_params: dict = {}
            alias_before_check = len(accumulated_alias)  # for _output prompt

            for param_name, default in params.items():
                ann = (
                    sig.parameters[param_name].annotation
                    if sig and param_name in sig.parameters
                    else inspect.Parameter.empty
                )

                if param_name in all_col_picker_params:
                    table_cols_set = set(table_cols)
                    alias_cols = alias_param_to_cols.get(param_name, [])
                    auto_match = param_name if param_name in table_cols_set else None

                    if alias_cols:
                        ordered = alias_cols + [c for c in table_cols if c not in alias_cols]
                        precheck_cols = set(alias_cols)
                        default_col = alias_cols[0]
                    else:
                        raw_history = get_column_param_history(conn, check_name, param_name)
                        historical_in_source = [c for c in raw_history if c in table_cols_set]
                        ordered = historical_in_source + [
                            c for c in table_cols if c not in historical_in_source
                        ]
                        precheck_cols = set(historical_in_source)
                        default_col = historical_in_source[0] if historical_in_source else None

                    if auto_match:
                        precheck_cols.add(auto_match)
                        if default_col is None:
                            default_col = auto_match

                    # pd.Series params never get the escape hatch — they are always
                    # column selectors by annotation. str/unannotated/int/float params
                    # get the escape hatch so the user can enter a broadcast constant.
                    has_escape = param_name not in series_param_set

                    if eligible:
                        click.echo(
                            f"  ℹ  Selecting multiple columns will run '{check_name}'"
                            f" once per column."
                        )
                        choices = _make_col_choices(ordered, registry_types, precheck=precheck_cols)
                        value = questionary.checkbox(f"{param_name}:", choices=choices).ask()
                        if value is None:
                            return [], [], True
                        value = sorted(value)
                        for col in value:
                            if not any(
                                e["param"] == param_name and e["column"] == col
                                for e in accumulated_alias
                            ):
                                accumulated_alias.append({"param": param_name, "column": col})
                    else:
                        col_choices = _make_col_choices(ordered, registry_types)
                        if has_escape:
                            col_choices = col_choices + [
                                questionary.Choice(title=_DIRECT_ENTRY, value=_DIRECT_ENTRY)
                            ]

                        value = questionary.select(
                            f"{param_name}:", choices=col_choices, default=default_col
                        ).ask()
                        if value is None:
                            return [], [], True

                        if value == _DIRECT_ENTRY:
                            raw = questionary.text(
                                f"{param_name} (enter value directly):"
                            ).ask()
                            if raw is None:
                                return [], [], True
                            if raw:
                                try:
                                    parsed: object = (
                                        int(raw) if "." not in raw else float(raw)
                                    )
                                except ValueError:
                                    parsed = raw
                            else:
                                parsed = raw
                            filled_params[param_name] = parsed
                            continue  # skip alias_map — broadcast constant

                        if not any(
                            e["param"] == param_name and e["column"] == value
                            for e in accumulated_alias
                        ):
                            accumulated_alias.append({"param": param_name, "column": value})

                    alias_param_to_cols[param_name] = [
                        e["column"] for e in accumulated_alias if e["param"] == param_name
                    ]

                    col_values = value if isinstance(value, list) else [value]
                    for col_val in col_values:
                        if col_val != _DIRECT_ENTRY:
                            record_param_history(
                                conn, check_name, report_name, table, {param_name: col_val}
                            )

                elif is_list_annotation(ann) or isinstance(default, list):
                    suggestions = get_param_suggestions(conn, check_name, param_name, table_cols)
                    ordered = suggestions + [c for c in table_cols if c not in suggestions]
                    choices = _make_col_choices(ordered, registry_types)
                    value = questionary.checkbox(f"{param_name}:", choices=choices).ask()
                    if value is None:
                        return [], [], True
                    filled_params[param_name] = sorted(value)

                else:
                    suggestions = get_param_suggestions(conn, check_name, param_name, table_cols)
                    suggested_default = (
                        suggestions[0]
                        if suggestions
                        else (str(default) if default is not inspect.Parameter.empty else "")
                    )
                    value = questionary.text(f"{param_name}:", default=suggested_default).ask()
                    if value is None:
                        return [], [], True
                    if value:
                        try:
                            value = int(value) if "." not in value else float(value)
                        except ValueError:
                            pass
                    filled_params[param_name] = value

            # ── DataFrame-return prompts ──────────────────────────────────────
            if has_df_return:
                if kind == "check":
                    click.echo(
                        f"\n  \u2139  '{check_name}' returns a DataFrame. Select the column "
                        f"that contains the boolean pass/fail values."
                    )
                    check_col = questionary.select(
                        "check_col — boolean column in the returned DataFrame:",
                        choices=_make_col_choices(table_cols, registry_types),
                    ).ask()
                    if check_col is None:
                        return [], [], True
                    filled_params["check_col"] = check_col

                elif kind == "transform":
                    click.echo(
                        f"\n  \u2139  '{check_name}' returns a DataFrame. Select the columns "
                        f"from the returned DataFrame that should overwrite the table."
                    )
                    overwrite_cols = questionary.checkbox(
                        "overwrite_cols — columns to write back to the table:",
                        choices=_make_col_choices(table_cols, registry_types),
                    ).ask()
                    if overwrite_cols is None:
                        return [], [], True
                    filled_params["overwrite_cols"] = sorted(overwrite_cols)

            # _output prompt for transforms with column-backed params
            # Fires when transform alias entries were added this check.
            # Checks never need _output — results go to validation_block.
            new_alias_entries = accumulated_alias[alias_before_check:]
            input_alias_entries = [
                e for e in new_alias_entries if e["param"] != "_output"
            ]
            if kind == "transform" and input_alias_entries:
                n_runs = max(
                    len([e for e in input_alias_entries if e["param"] == p])
                    for p in {e["param"] for e in input_alias_entries}
                )
                click.echo(
                    f"\n  \u2139  '{check_name}' is a transform. "
                    f"Select the output column(s) where each run's result "
                    f"should be written ({n_runs} run(s) — select {n_runs} to align)."
                )
                if n_runs == 1:
                    output_val = questionary.select(
                        "Output column:",
                        choices=_make_col_choices(table_cols, registry_types),
                    ).ask()
                    if output_val is None:
                        return [], [], True
                    accumulated_alias.append({"param": "_output", "column": output_val, "check": check_name})
                else:
                    # Step 1: which param's column list should the output mirror?
                    # Only multi-column params (>1 entry) are offered as choices —
                    # single-entry params just broadcast and aren't meaningful to mirror.
                    _MANUAL_SELECT = "\u270e Select output columns manually"
                    multi_col_params = {
                        p: [e["column"] for e in input_alias_entries if e["param"] == p]
                        for p in {e["param"] for e in input_alias_entries}
                        if len([e for e in input_alias_entries if e["param"] == p]) > 1
                    }
                    param_choices = [
                        questionary.Choice(
                            title=f"{p}  [{', '.join(cols)}]",
                            value=p,
                        )
                        for p, cols in multi_col_params.items()
                    ] + [questionary.Choice(title=_MANUAL_SELECT, value=_MANUAL_SELECT)]

                    mirror_param = questionary.select(
                        "Output columns — tie to which param's column list?",
                        choices=param_choices,
                    ).ask()
                    if mirror_param is None:
                        return [], [], True

                    if mirror_param != _MANUAL_SELECT:
                        # Param chosen → directly use that param's columns as output.
                        # No second prompt: step 1 IS the selection. A second checkbox
                        # causes Enter-key bleed-through (the Enter that closed step 1
                        # immediately submits step 2's pre-checked items).
                        for col in multi_col_params[mirror_param]:
                            accumulated_alias.append({"param": "_output", "column": col, "check": check_name})
                    else:
                        # Manual → user picks output columns freely from full column list.
                        output_choices = _make_col_choices(table_cols, registry_types)
                        output_vals = questionary.checkbox(
                            "Output columns:",
                            choices=output_choices,
                        ).ask()
                        if output_vals is None:
                            return [], [], True
                        for col in output_vals:
                            accumulated_alias.append({"param": "_output", "column": col, "check": check_name})

            record_param_history(conn, check_name, report_name, table, filled_params)
            entry = {"name": check_name}
            if filled_params:
                entry["params"] = filled_params
            check_entries.append(entry)

        return check_entries, accumulated_alias, False


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
        from proto_pipe.cli.scaffold import build_rich_sql_scaffold

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
            scaffold = build_rich_sql_scaffold(
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


# ---------------------------------------------------------------------------
def display_name(check_name: str, check_registry) -> str:
    """Resolve a check UUID to the original function __name__.

    Belongs in prompts.py — this is a display concern, not a business logic
    concern. Business logic modules pass check_name (UUID) and the CLI layer
    resolves it for human-readable output.

    Reads func_name from CheckContract — resolved once at validate_check time
    per the inspect-once principle. Never re-inspects the function or its
    partial chain at call time.

    Falls back to the UUID unchanged if the contract is unavailable.
    """
    try:
        contract = check_registry.get_contract(check_name)
        name = contract.func_name
        return name if name else check_name
    except Exception:
        return check_name


# ---------------------------------------------------------------------------
# Progress reporters — rich spinner base + per-pipeline subclasses
# ---------------------------------------------------------------------------

class PipelineProgressReporter:
    """Abstract base class for rich spinner progress reporters.

    Owns the rich Progress lifecycle (__enter__/__exit__), the console print
    method, and the spinner update method. Subclasses add pipeline-specific
    callbacks (on_file_start/done, on_report_start/done, etc.).

    Never instantiated directly — always use a concrete subclass.

    DRY: spinner setup (SpinnerColumn + TextColumn + TimeElapsedColumn +
    transient=True) is defined once here and inherited by all reporters.
    Adding a new reporter for deliverables or other pipelines requires only
    adding the pipeline-specific callbacks — no spinner boilerplate.
    """

    def __init__(self) -> None:
        self._progress = None
        self._task = None

    def __enter__(self) -> "PipelineProgressReporter":
        from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            transient=True,
        )
        self._progress.start()
        self._task = self._progress.add_task(self._initial_description(), total=None)
        return self

    def __exit__(self, *_args) -> None:
        if self._progress:
            self._progress.stop()

    def _initial_description(self) -> str:
        """Override in subclass to set the initial spinner text."""
        return "Working..."

    def _update_spinner(self, description: str) -> None:
        """Update the spinner text. Safe to call before __enter__."""
        if self._progress is not None and self._task is not None:
            self._progress.update(self._task, description=description)

    def _print(self, message: str) -> None:
        """Print a persistent line via rich console (survives spinner clearing)."""
        if self._progress is not None:
            self._progress.console.print(message)


class IngestProgressReporter(PipelineProgressReporter):
    """Rich progress display for vp ingest.

    Displays a spinner with the current filename and elapsed time while each
    file is being processed. Extends PipelineProgressReporter — spinner
    lifecycle and print helpers are inherited.

    Usage in cli/data.py:
        with IngestProgressReporter() as reporter:
            summary = ingest_directory(
                ...,
                on_file_start=reporter.on_file_start,
                on_file_done=reporter.on_file_done,
            )

    No rich imports in ingest.py — callbacks are plain callables, zero
    coupling between the business logic and the display layer.
    """

    def __init__(self) -> None:
        super().__init__()
        self._ok = 0
        self._failed = 0
        self._skipped = 0

    def _initial_description(self) -> str:
        return "Waiting for files..."

    def on_file_start(self, filename: str) -> None:
        """Called by ingest_directory before each file is processed."""
        self._update_spinner(f"[cyan]{filename}[/cyan]")

    def on_file_done(self, filename: str, result: dict) -> None:
        """Called by ingest_directory after each file completes."""
        status = result.get("status", "")
        rows = result.get("rows") or 0
        flagged = result.get("flagged") or 0
        message = result.get("message") or ""

        if status == "ok":
            self._ok += 1
            parts = [f"{rows} row(s) loaded"]
            if flagged:
                parts.append(f"{flagged} blocked")
            self._print(f"  [ok] {filename} ({', '.join(parts)})")
        elif status == "failed":
            self._failed += 1
            self._print(f"  [error] {filename}: {message}")
        elif status == "skipped":
            self._skipped += 1
            self._print(f"  [skip] {filename}")


class ValidateProgressReporter(PipelineProgressReporter):
    """Rich streaming progress display for vp validate.

    Displays a spinner showing the current report being validated. Each time
    a report completes, a persistent colored result line is printed and the
    spinner moves to the next report. Check-level results (pass/fail/error)
    are printed under each report line.

    Color conventions per CLAUDE.md:
      passed  → green ✓
      failed  → bold red ✗ + failure count
      error   → bold red
      skipped → dim

    Usage in cli/commands/validation.py:
        with ValidateProgressReporter(check_registry) as reporter:
            results = run_all_reports(
                ...,
                on_report_done=reporter.on_report_done,
            )

    No rich imports in reports/runner.py — callback is a plain callable,
    zero coupling between business logic and the display layer.
    """

    def __init__(self, check_registry) -> None:
        super().__init__()
        self._check_registry = check_registry
        self._completed = 0
        self._failed = 0
        self._skipped = 0

    def _initial_description(self) -> str:
        return "Starting validation..."

    def on_report_start(self, report_name: str) -> None:
        """Called by run_all_reports before each report is processed."""
        self._update_spinner(f"[cyan]{report_name}[/cyan]")

    def on_report_done(self, result: dict) -> None:
        """Called by run_all_reports after each report completes."""
        report_name = result.get("report", "")
        status = result.get("status", "")

        if status == "completed":
            self._completed += 1
            results = result.get("results", {})
            has_failures = any(
                v.get("status") in ("failed", "error") for v in results.values()
            )
            header_style = "bold red" if has_failures else "bold green"
            self._print(f"\n  [{header_style}]{report_name}[/{header_style}] [completed]")

            for check_name, outcome in results.items():
                name = display_name(check_name, self._check_registry)
                check_status = outcome.get("status", "")
                failed_count = outcome.get("failed_count", 0)

                if check_status == "passed":
                    self._print(f"    [green]✓[/green] {name}")
                elif check_status == "failed":
                    self._print(f"    [bold red]✗[/bold red] {name}")
                    self._print(
                        f"      [dim]{failed_count} row(s) failed → validation_block[/dim]"
                    )
                elif check_status == "error":
                    self._print(f"    [bold red]✗ {name}: {outcome.get('error', '')}[/bold red]")

        elif status == "skipped":
            self._skipped += 1
            self._print(f"\n  [dim]{report_name} [skipped — no pending records][/dim]")

        elif status == "error":
            self._failed += 1
            self._print(
                f"\n  [bold red]{report_name} [error]: {result.get('error', '')}[/bold red]"
            )


# IngestProgressReporter — rich spinner for vp ingest
# ---------------------------------------------------------------------------
