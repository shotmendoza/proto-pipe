"""CLI prompts — SourceConfigPrompter, ReportConfigPrompter, DeliverableConfigPrompter.

Each prompter owns all questionary interactions for its resource type.
No DB connections, no business logic — pure UI.

Usage pattern:
    prompter = SourceConfigPrompter(sample_df=df, registry_hints=hints)
    if not prompter.run(existing_names, suggested_pattern):
        return  # cancelled
    config.add_or_update(prompter.source)
    write_registry_types(conn, prompter.source[ "name"], prompter.confirmed_types)
"""
from __future__ import annotations

import click
import questionary

from dataclasses import dataclass

from proto_pipe.constants import DUCKDB_TYPES, DATE_FORMATS
from proto_pipe.reports.callbacks import ReportCallback, LogEntry


@dataclass
class MacroApplication:
    """A macro bound to specific columns/literals for a deliverable query.

    Built by DeliverableConfigPrompter.prompt_all_macros().
    Consumed by build_deliverable_sql() in scaffold.py for SQL generation.
    """

    macro_name: str  # e.g. "apply_quota_share"
    param_bindings: dict[str, str]  # param_name → column_name or literal value
    output_column: str  # alias in SELECT clause
    overwrites: str | None  # column name it replaces, or None if new


def prompt_custom_export_path() -> "Path | None":
    """Prompt for a custom CSV export path with a pipeline warning.

    Shows a warning that custom paths are not picked up by vp ingest or
    vp errors source retry, then asks for a full output path from the user.

    Returns a resolved Path, or None if the user cancels.
    """
    from pathlib import Path

    click.echo(
        "\n[warn] Custom paths won't be picked up by vp ingest or vp errors source retry. "
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

        # If there are file columns but no types were confirmed, the user
        # cancelled mid-flow inside prompt_column_types — treat as cancel.
        if self._file_cols and not self.confirmed_types:
            return False

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


def prompt_param_binding(
    param_name: str,
    param_type: type | None,
    available_columns: list[str],
) -> str | None:
    """Prompt user to bind a parameter to a column or a literal value.

    Used by both DeliverableConfigPrompter (macro param binding) and
    ReportConfigPrompter._fill_params (column-or-literal escape hatch).

    Returns the column name or literal string, or None if cancelled.
    """
    import questionary

    choices = [questionary.Choice(col, value=col) for col in available_columns]
    choices.append(questionary.Choice(title=_DIRECT_ENTRY, value=_DIRECT_ENTRY))

    value = questionary.select(f"{param_name}:", choices=choices).ask()
    if value is None:
        return None

    if value == _DIRECT_ENTRY:
        type_hint = f" ({param_type.__name__})" if param_type else ""
        raw = questionary.text(f"{param_name}{type_hint} (enter value directly):").ask()
        if raw is None:
            return None
        return raw

    return value


# Sentinel for the free-text escape hatch in column-picker prompts.
# When selected, the user types a value directly → filled_params (broadcast constant)
# rather than alias_map (column-backed per-row). Never stored in alias_map.
_DIRECT_ENTRY = "\u270e Enter value directly"


def _ask_skip_or_abort(check_name: str) -> bool:
    """Prompt the user to skip a function or abort the entire wizard.

    Returns True to skip (caller should continue to next function),
    False to abort (caller should return go_back=True).
    """
    skip = questionary.confirm(
        f"  Skip '{check_name}' and continue with remaining functions?",
        default=True,
    ).ask()
    # None (ESC on the confirm itself) → treat as abort
    return bool(skip)


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
        from proto_pipe.io.db import get_table_columns

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

        ESC during any param prompt offers to skip the current function and
        continue with the remaining selections, or abort back to check selection.

        Returns (check_entries, alias_map_entries, go_back).
        """
        import inspect
        from proto_pipe.cli.scaffold import (
            get_original_func,
            get_check_params,
            is_list_annotation,
        )
        from proto_pipe.io.db import (
            get_table_columns,
            get_param_suggestions,
            get_column_param_history,
            record_param_history,
            get_registry_types,
        )

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

            # Issue 3 fix: gate multi-select on is_expandable().
            # Only functions returning pd.Series or pd.DataFrame can be
            # meaningfully expanded N times via alias_map. Without this gate,
            # the prompt allows multi-column selection but the runtime
            # (_expand_check_with_alias_map) silently ignores it.
            eligible = (
                self._multi_select
                and inspector is not None
                and inspector.is_expandable()
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
            alias_before_check = len(accumulated_alias)  # for rollback on skip
            skip_this_check = False  # Issue 4: flag for skip-and-continue

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
                        # Issue 2 fix: escape hatch in checkbox path for
                        # column-backed scalars so user can enter a constant.
                        if has_escape:
                            choices.append(
                                questionary.Choice(
                                    title=_DIRECT_ENTRY, value=_DIRECT_ENTRY
                                )
                            )
                        value = questionary.checkbox(f"{param_name}:", choices=choices).ask()
                        if value is None:
                            if _ask_skip_or_abort(check_name):
                                skip_this_check = True
                                break
                            return [], [], True

                        # Issue 2: if user selected _DIRECT_ENTRY in checkbox,
                        # route to free-text and store as broadcast constant.
                        if _DIRECT_ENTRY in value:
                            raw = questionary.text(
                                f"{param_name} (enter value directly):"
                            ).ask()
                            if raw is None:
                                if _ask_skip_or_abort(check_name):
                                    skip_this_check = True
                                    break
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
                                questionary.Choice(
                                    title=_DIRECT_ENTRY, value=_DIRECT_ENTRY
                                )
                            ]

                        value = questionary.select(
                            f"{param_name}:",
                            choices=col_choices,
                            default=default_col,
                        ).ask()
                        if value is None:
                            if _ask_skip_or_abort(check_name):
                                skip_this_check = True
                                break
                            return [], [], True

                        if value == _DIRECT_ENTRY:
                            raw = questionary.text(
                                f"{param_name} (enter value directly):"
                            ).ask()
                            if raw is None:
                                if _ask_skip_or_abort(check_name):
                                    skip_this_check = True
                                    break
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
                        if _ask_skip_or_abort(check_name):
                            skip_this_check = True
                            break
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
                        if _ask_skip_or_abort(check_name):
                            skip_this_check = True
                            break
                        return [], [], True
                    if value:
                        try:
                            value = int(value) if "." not in value else float(value)
                        except ValueError:
                            pass
                    filled_params[param_name] = value

            # Issue 4: roll back alias entries and skip to next function
            if skip_this_check:
                accumulated_alias = accumulated_alias[:alias_before_check]
                continue

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
                        if _ask_skip_or_abort(check_name):
                            accumulated_alias = accumulated_alias[:alias_before_check]
                            continue
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
                        if _ask_skip_or_abort(check_name):
                            accumulated_alias = accumulated_alias[:alias_before_check]
                            continue
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
                        if _ask_skip_or_abort(check_name):
                            accumulated_alias = accumulated_alias[:alias_before_check]
                            continue
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
                        if _ask_skip_or_abort(check_name):
                            accumulated_alias = accumulated_alias[:alias_before_check]
                            continue
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
                            if _ask_skip_or_abort(check_name):
                                accumulated_alias = accumulated_alias[:alias_before_check]
                                continue
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


def _flatten_columns_for_macros(
    all_columns: dict[str, list[str]],
) -> list[str]:
    """Flatten table columns into a single list for macro param binding.

    When a column name exists in multiple tables, includes qualified
    names (table.column) so the user can disambiguate.
    """
    col_counts: dict[str, int] = {}
    for cols in all_columns.values():
        for col in cols:
            col_counts[col] = col_counts.get(col, 0) + 1

    result: list[str] = []
    seen: set[str] = set()

    for table, cols in all_columns.items():
        for col in cols:
            if col_counts[col] > 1:
                qualified = f"{table}.{col}"
                if qualified not in seen:
                    result.append(qualified)
                    seen.add(qualified)
            else:
                if col not in seen:
                    result.append(col)
                    seen.add(col)

    return result


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
            macro_registry=None,
    ) -> None:
        self._rep_config = rep_config
        self._src_config = src_config
        self._sql_dir = sql_dir
        self._existing = existing_deliverable or {}
        self._macro_registry = macro_registry  # MacroRegistry or None
        # Output property — only valid after run() returns True
        self.deliverable: dict = {}

    # ---------------------------------------------------------------------------
    # run() — full deliverable config flow
    # ---------------------------------------------------------------------------
    def run(
        self,
        existing_names: list[str],
        available_reports: list[str],
        pipeline_db: str | None = None,
        views_config_path: str | None = None,
    ) -> bool:
        """Run the full deliverable configuration flow.

        Stores the completed deliverable dict in self.deliverable.
        Returns True on completion, False if cancelled.
        """
        import click
        from proto_pipe.cli.scaffold import (
            DeliverableSQLSpec,
            build_deliverable_sql,
        )

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

        # Column selection + SQL generation (when pipeline_db available)
        sql_file = None
        if pipeline_db:
            report_columns = self.prompt_columns(selected_reports, pipeline_db)
            if report_columns is None:
                return False

            # View selection (optional — never cancels the wizard)
            selected_views = self.prompt_views(views_config_path)
            view_columns: dict[str, list[str]] = {}
            if selected_views:
                view_columns_result = self.prompt_view_columns(
                    selected_views, pipeline_db
                )
                if view_columns_result is None:
                    return False
                view_columns = view_columns_result

            # Macro selection (optional — skips if no macros available)
            all_columns = {**report_columns, **view_columns}
            all_col_names = _flatten_columns_for_macros(all_columns)
            macro_applications = self.prompt_all_macros(
                self._macro_registry,
                all_col_names,
            )

            # Join configuration — unified for reports + views
            join_specs = []
            if len(all_columns) > 1:
                join_specs = self.prompt_joins(all_columns)
                if join_specs is None:
                    return False

            order_by = self.prompt_order_by(report_columns)

            spec = DeliverableSQLSpec(
                deliverable_name=name,
                report_columns=report_columns,
                join_specs=join_specs,
                order_by=order_by,
                view_columns=view_columns,
                macro_applications=macro_applications,
            )

            sql_content = build_deliverable_sql(spec)

            # Edit flow: confirm before overwriting hand-edited SQL
            existing_sql = self._existing.get("sql_file")
            if existing_sql:
                from pathlib import Path as _Path

                if _Path(existing_sql).exists():
                    proceed = questionary.confirm(
                        f"This will regenerate {existing_sql}. "
                        f"Any hand edits will be lost. Continue?",
                        default=True,
                    ).ask()
                    if not proceed:
                        sql_file = existing_sql
                        click.echo(f"  [skip] Keeping existing {existing_sql}")
                    else:
                        # Write directly — already confirmed, skip _write_sql_file's
                        # own overwrite check to avoid a double confirmation.
                        sql_path = _Path(self._sql_dir) / f"{name}.sql"
                        sql_path.parent.mkdir(parents=True, exist_ok=True)
                        sql_path.write_text(sql_content)
                        click.echo(f"  [ok]   {sql_path}")
                        sql_file = str(sql_path)
                else:
                    sql_file = self._write_sql_file(name, sql_content)
            else:
                sql_file = self._write_sql_file(name, sql_content)
        else:
            # Fallback: prompt-based SQL when no DB available
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

    # --- New methods to add to DeliverableConfigPrompter ---

    def prompt_columns(
        self,
        selected_reports: list[str],
        pipeline_db: str,
    ) -> dict[str, list[str]] | None:
        """For each selected report, show its columns and let the user pick.

        Returns {report_name: [selected_columns]} or None if cancelled.
        Uses get_table_columns from io/db.py (Rule 15 — reuse existing).
        """
        from proto_pipe.io.db import get_table_columns

        report_columns: dict[str, list[str]] = {}

        for report_name in selected_reports:
            columns = get_table_columns(pipeline_db, report_name)
            if not columns:
                click.echo(
                    f"\n  [warn] No columns found for report '{report_name}'."
                    f" Has 'vp validate' been run?"
                )
                continue

            selected = questionary.checkbox(
                f"Columns from '{report_name}':",
                choices=[
                    questionary.Choice(col, value=col, checked=True) for col in columns
                ],
            ).ask()

            if selected is None:
                return None
            if not selected:
                click.echo(
                    f"  [warn] No columns selected for '{report_name}' — skipping."
                )
                continue

            report_columns[report_name] = selected

        if not report_columns:
            click.echo("  No columns selected from any report.")
            return None

        return report_columns

    def prompt_joins(
        self,
        all_columns: dict[str, list[str]],
    ) -> list | None:
        """Prompt for join configuration between tables.

        For each table after the first, asks:
        - Which already-configured table to join against
        - Which column in the left table is the join key
        - Which column in the right table is the join key
        - What type of join (LEFT, INNER, FULL OUTER)

        Works for reports, views, or any mix. The first table in
        all_columns is the base — every other table must join to
        one of the tables that came before it.

        Returns list[JoinSpec] or None if cancelled.
        """
        from proto_pipe.cli.scaffold import JoinSpec

        tables = list(all_columns.keys())
        if len(tables) < 2:
            return []

        join_specs = []

        for i, right_table in enumerate(tables[1:], start=1):
            # Tables available to join against = all tables before this one
            available_left = tables[:i]

            if len(available_left) == 1:
                left_table = available_left[0]
            else:
                left_table = questionary.select(
                    f"  Join '{right_table}' to which table?",
                    choices=available_left,
                ).ask()
                if left_table is None:
                    return None

            click.echo(f"\n  Join: {left_table} → {right_table}")

            # Left key
            left_cols = all_columns[left_table]
            left_key = questionary.select(
                f"  Join key in '{left_table}':",
                choices=left_cols,
            ).ask()
            if left_key is None:
                return None

            # Right key
            right_cols = all_columns[right_table]
            right_key = questionary.select(
                f"  Matching key in '{right_table}':",
                choices=right_cols,
            ).ask()
            if right_key is None:
                return None

            # Join type
            join_type = questionary.select(
                "  Join type:",
                choices=[
                    questionary.Choice(
                        "LEFT JOIN — all rows from left, matching from right",
                        value="LEFT",
                    ),
                    questionary.Choice(
                        "INNER JOIN — only matching rows", value="INNER"
                    ),
                    questionary.Choice(
                        "FULL OUTER JOIN — all rows from both", value="FULL OUTER"
                    ),
                ],
                default="LEFT",
            ).ask()
            if join_type is None:
                return None

            join_specs.append(
                JoinSpec(
                    left_table=left_table,
                    right_table=right_table,
                    left_key=left_key,
                    right_key=right_key,
                    join_type=join_type,
                )
            )

        return join_specs

    def prompt_views(self, views_config_path: str | None) -> list[str]:
        """Present available views as a checkbox and return selected names.

        Returns empty list if no views exist or none selected.
        Never returns None — views are optional, not cancellable.
        """
        if not views_config_path:
            return []

        from proto_pipe.reports.views import load_views_config  # TODO: This likely needs to be moved

        views = load_views_config(views_config_path)
        if not views:
            return []

        view_names = [v["name"] for v in views]
        selected = questionary.checkbox(
            "Include views in this deliverable?",
            choices=[questionary.Choice(name, value=name) for name in view_names],
        ).ask()

        return selected or []

    def prompt_order_by(
        self,
        report_columns: dict[str, list[str]],
    ) -> str | None:
        """Prompt for an optional ORDER BY column.

        Shows columns from the first (base) report. Returns column name or None.
        """
        reports = list(report_columns.keys())
        if not reports:
            return None

        base_cols = report_columns[reports[0]]
        add_order = questionary.confirm("Add an ORDER BY clause?", default=False).ask()
        if not add_order:
            return None

        col = questionary.select(
            "Order by which column?",
            choices=base_cols,
        ).ask()

        return col

    def _write_sql_file(self, name: str, sql_content: str) -> str:
        """Write generated SQL to the sql_dir. Returns the file path as string."""
        from pathlib import Path

        sql_path = Path(self._sql_dir) / f"{name}.sql"
        sql_path.parent.mkdir(parents=True, exist_ok=True)

        if sql_path.exists():
            overwrite = questionary.confirm(
                f"{sql_path} already exists. Overwrite?"
            ).ask()
            if not overwrite:
                click.echo(f"  [skip] Keeping existing {sql_path}")
                return str(sql_path)

        sql_path.write_text(sql_content)
        click.echo(f"  [ok]   {sql_path}")
        return str(sql_path)

    def prompt_view_columns(
        self,
        selected_views: list[str],
        pipeline_db: str,
    ) -> dict[str, list[str]] | None:
        """For each selected view, show its columns and let the user pick.

        Returns {view_name: [selected_columns]} or None if cancelled.
        Excludes _-prefixed internal columns.
        Uses get_table_columns from io/db.py (works on views too).
        """
        from proto_pipe.io.db import get_table_columns

        view_columns: dict[str, list[str]] = {}

        for view_name in selected_views:
            columns = get_table_columns(pipeline_db, view_name)
            # Exclude internal columns
            columns = [c for c in columns if not c.startswith("_")]

            if not columns:
                click.echo(
                    f"\n  [warn] No columns found for view '{view_name}'."
                    f" Has the view been created? Run: vp refresh views"
                )
                continue

            selected = questionary.checkbox(
                f"Columns from view '{view_name}':",
                choices=[
                    questionary.Choice(col, value=col, checked=True) for col in columns
                ],
            ).ask()

            if selected is None:
                return None
            if not selected:
                click.echo(
                    f"  [warn] No columns selected for view '{view_name}' — skipping."
                )
                continue

            view_columns[view_name] = selected

        return view_columns


    def prompt_macros(self, macro_registry) -> list[str]:
        """Checkbox of available macros. Returns selected macro names.

        Skips silently if macro_registry is None or empty.
        Never returns None — macros are optional, not cancellable.
        """
        if macro_registry is None:
            return []

        available = macro_registry.available()
        if not available:
            return []

        choices = []
        for name in available:
            contract = macro_registry.get_contract(name)
            if contract:
                sig = f"{contract.func_name}({', '.join(contract.params)})"
                choices.append(questionary.Choice(sig, value=name))
            else:
                choices.append(questionary.Choice(name, value=name))

        selected = questionary.checkbox(
            "Apply macros to this deliverable?",
            choices=choices,
        ).ask()

        return selected or []

    def prompt_macro_params(
        self,
        contract,
        available_columns: list[str],
    ) -> dict[str, str] | None:
        """For each param in the macro contract, prompt for a column or literal.

        Returns {param_name: column_name_or_literal} or None if cancelled.
        """
        bindings: dict[str, str] = {}
        for param_name in contract.params:
            param_type = contract.param_types.get(param_name)
            value = prompt_param_binding(param_name, param_type, available_columns)
            if value is None:
                return None
            bindings[param_name] = value
        return bindings

    def prompt_macro_output(
        self,
        macro_name: str,
        available_columns: list[str],
    ) -> tuple[str, str | None] | None:
        """Prompt for output column name and whether it overwrites an existing column.

        Returns (output_column, overwrites) or None if cancelled.
        overwrites is the column name being replaced, or None for a new column.
        """
        NEW_COL = "\u271a New column"
        choices = [questionary.Choice(col, value=col) for col in available_columns]
        choices.append(questionary.Choice(title=NEW_COL, value=NEW_COL))

        click.echo(f"\n  Output for '{macro_name}':")
        value = questionary.select(
            "  Overwrite an existing column, or add a new one?",
            choices=choices,
        ).ask()
        if value is None:
            return None

        if value == NEW_COL:
            col_name = questionary.text(
                f"  New column name for {macro_name}:"
            ).ask()
            if not col_name:
                return None
            return (col_name, None)

        return (value, value)

    def prompt_all_macros(
        self,
        macro_registry,
        available_columns: list[str],
    ) -> list:
        """Orchestrate macro selection, param binding, and output for each.

        Returns list[MacroApplication]. Empty list if no macros selected.
        """
        selected_names = self.prompt_macros(macro_registry)
        if not selected_names:
            return []

        applications = []
        for name in selected_names:
            contract = macro_registry.get_contract(name)
            if contract is None:
                continue

            click.echo(f"\n\u2500\u2500 Macro: {contract.func_name} \u2500\u2500")

            bindings = self.prompt_macro_params(contract, available_columns)
            if bindings is None:
                return []

            output = self.prompt_macro_output(name, available_columns)
            if output is None:
                return []

            output_column, overwrites = output

            applications.append(
                MacroApplication(
                    macro_name=name,
                    param_bindings=bindings,
                    output_column=output_column,
                    overwrites=overwrites,
                )
            )

        return applications

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


class ViewConfigPrompter:
    """Collects user input for creating a new view configuration.

    Single-use: create a fresh instance per command invocation.
    After run() returns True, read results from self.view_config,
    self.sql_spec, and self.insert_after.
    """

    def __init__(self) -> None:
        # Output properties — only valid after run() returns True
        self.view_config: dict = {}  # {"name": ..., "sql_file": ...}
        self.sql_spec = None  # ViewSQLSpec
        self.insert_after: str | None = None  # view name for dependency ordering

    # ---------------------------------------------------------------------------
    # run() — full view config flow
    # ---------------------------------------------------------------------------

    def run(
        self,
        existing_view_names: list[str],
        available_tables: list,
        pipeline_db: str,
    ) -> bool:
        """Run the full view configuration flow.

        Stores results in self.view_config, self.sql_spec, self.insert_after.
        Returns True on completion, False if cancelled.

        :param existing_view_names: Names of already-configured views.
        :param available_tables:    list[str] or list[tuple[str, int]] from
                                    get_all_source_tables.
        :param pipeline_db:         Path to pipeline DB for column discovery.
        """
        from proto_pipe.cli.scaffold import ViewSQLSpec
        from proto_pipe.io.db import get_table_columns

        self.view_config = {}
        self.sql_spec = None
        self.insert_after = None

        name = self.prompt_name(existing_view_names)
        if not name:
            return False

        table = self.prompt_base_table(available_tables)
        if not table:
            return False

        view_type = self.prompt_view_type()
        if not view_type:
            return False

        group_by_columns: list[str] = []
        aggregate_functions: dict[str, str] = {}
        where_clause: str = ""

        if view_type == "aggregate":
            table_columns = get_table_columns(pipeline_db, table)
            if not table_columns:
                click.echo(f"\n  [error] No columns found in table '{table}'.")
                return False

            group_by_columns = self.prompt_group_by_columns(table_columns)
            if not group_by_columns:
                return False

            non_grouped = [c for c in table_columns if c not in group_by_columns]
            if non_grouped:
                aggregate_functions = self.prompt_aggregate_functions(non_grouped)
                if aggregate_functions is None:
                    return False

        elif view_type == "filter":
            where_clause = self.prompt_where_clause()
            if where_clause is None:
                return False

        # Dependency ordering
        if existing_view_names:
            self.insert_after = self.prompt_view_dependency(existing_view_names)

        self.sql_spec = ViewSQLSpec(
            view_name=name,
            base_table=table,
            view_type=view_type,
            group_by_columns=group_by_columns,
            aggregate_functions=aggregate_functions,
            where_clause=where_clause or "",
        )
        self.view_config = {"name": name, "sql_file": ""}  # caller sets sql_file path
        return True

    def prompt_name(self, existing_view_names: list[str]) -> str | None:
        """Prompt for view name. Returns None if cancelled."""
        name = questionary.text("View name (e.g. carrier_summary):").ask()
        if not name:
            return None

        if name in existing_view_names:
            overwrite = questionary.confirm(
                f"View '{name}' already exists. Overwrite?"
            ).ask()
            if not overwrite:
                return None

        return name

    def prompt_base_table(self, available_tables) -> str | None:
        """Prompt for base table selection. Returns None if cancelled.

        Accepts list[str] or list[tuple[str, int]] from get_all_source_tables.
        When tuples are provided, annotated questionary.Choice objects are
        built here — keeping all questionary formatting inside prompts.py.
        """
        if available_tables and isinstance(available_tables[0], tuple):
            choices = [
                questionary.Choice(
                    title=(
                        f"{name}  ({count} report{'s' if count != 1 else ''})"
                        if count > 0
                        else name
                    ),
                    value=name,
                )
                for name, count in available_tables
            ]
        else:
            choices = list(available_tables)
        return questionary.select(
            "Which table should this view query?",
            choices=choices,
        ).ask()

    def prompt_view_type(self) -> str | None:
        """Prompt for view type selection. Returns None if cancelled."""
        return questionary.select(
            "View type:",
            choices=[
                questionary.Choice(
                    "Aggregate — GROUP BY with SUM, COUNT, etc.",
                    value="aggregate",
                ),
                questionary.Choice(
                    "Filter    — SELECT * WHERE <condition>",
                    value="filter",
                ),
                questionary.Choice(
                    "Custom    — blank template to edit manually",
                    value="custom",
                ),
            ],
        ).ask()

    def prompt_group_by_columns(self, table_columns: list[str]) -> list[str] | None:
        """Prompt for GROUP BY column selection. Returns None if cancelled."""
        selected = questionary.checkbox(
            "Select columns to GROUP BY:",
            choices=[questionary.Choice(col, value=col) for col in table_columns],
        ).ask()
        if not selected:
            click.echo("  Please select at least one GROUP BY column.")
            return None
        return selected

    def prompt_aggregate_functions(
        self,
        non_grouped_columns: list[str],
    ) -> dict[str, str] | None:
        """Prompt for aggregate function per non-grouped column.

        Returns dict mapping column name → aggregate function name.
        Columns assigned 'exclude' are omitted from the result.
        Returns None if cancelled.
        """
        agg_choices = ["SUM", "COUNT", "AVG", "MIN", "MAX", "exclude"]
        result: dict[str, str] = {}

        for col in non_grouped_columns:
            agg = questionary.select(
                f"Aggregate for '{col}':",
                choices=agg_choices,
            ).ask()
            if agg is None:
                return None
            if agg != "exclude":
                result[col] = agg

        return result

    def prompt_where_clause(self) -> str | None:
        """Prompt for WHERE clause text. Returns None if cancelled."""
        clause = questionary.text(
            "WHERE clause (e.g. status = 'active'):",
        ).ask()
        return clause

    def prompt_view_dependency(self, existing_view_names: list[str]) -> str | None:
        """Ask if this view depends on another view. Returns view name or None."""
        depends = questionary.confirm(
            "Does this view reference another view?",
            default=False,
        ).ask()
        if not depends:
            return None

        return questionary.select(
            "Insert after which view?",
            choices=existing_view_names,
        ).ask()


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
                v.status in ("failed", "error") for v in results.values()
            )
            header_style = "bold red" if has_failures else "bold green"
            self._print(f"\n  [{header_style}]{report_name}[/{header_style}] [completed]")

            for check_name, outcome in results.items():
                name = display_name(check_name, self._check_registry)
                check_status = outcome.status
                failed_count = (
                    int(outcome.result.mask.sum())
                    if outcome.result and outcome.result.mask is not None
                    else 0
                )

                if check_status == "passed":
                    self._print(f"    [green]✓[/green] {name}")
                elif check_status == "failed":
                    self._print(f"    [bold red]✗[/bold red] {name}")
                    self._print(
                        f"      [dim]{failed_count} row(s) failed → validation_block[/dim]"
                    )
                elif check_status == "error":
                    self._print(f"    [bold red]✗ {name}: {outcome.error or ''}[/bold red]")

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


# ---------------------------------------------------------------------------
# ValidateReportCallback — structured log rendering for vp validate
#
# ADD THIS after the ValidateProgressReporter class in cli/prompts.py.
# This renders LogEntry items from the compute phase via rich console.
# ---------------------------------------------------------------------------

class ValidateReportCallback(ReportCallback):
    """Render structured LogEntry items from report computation via rich.

    Used by vp validate to display compute-phase progress (pending counts,
    check failures, transform warnings) and write-phase notifications
    (table creation, deliverable writes).

    Instantiated in cli/commands/validation.py, passed as callback= to
    run_all_reports. The reports layer never imports this class — it only
    sees the base ReportCallback type.
    """

    def __init__(self, check_registry) -> None:
        self._check_registry = check_registry

    def _resolve_name(self, check_name: str) -> str:
        """Resolve check UUID to human-readable function name."""
        return display_name(check_name, self._check_registry)

    def on_log(self, entry: "LogEntry") -> None:
        """Render a structured log entry to the terminal."""
        level = entry.level
        category = entry.category

        if category == "skip":
            click.echo(f"  [skip] {entry.message}")
        elif category == "pending":
            click.echo(
                f"  [{entry.report_name}] {entry.count} pending record(s) of "
                f"{entry.total} total"
            )
        elif category == "check" and level == "info":
            name = self._resolve_name(entry.check_name) if entry.check_name else ""
            click.echo(
                f"  [check] '{name}' — {entry.count} failure(s) → validation_block"
            )
        elif category == "check" and level == "error":
            click.echo(f"  [check-fail] {entry.message}")
        elif category == "transform" and level == "warn":
            click.echo(f"  [transform-warn] {entry.message}")
        elif category == "transform" and level == "error":
            click.echo(f"  [transform-fail] {entry.message}")
        elif category == "transform" and level == "info":
            click.echo(f"  [transform] {entry.message}")
        elif category == "check_set_changed":
            click.echo(f"  [warn] {entry.message}")
        else:
            click.echo(f"  [{level}] {entry.message}")

    def on_write_done(
        self, report_name: str, target_table: str, row_count: int, first_run: bool
    ) -> None:
        if first_run:
            click.echo(
                f"  [ok] Created report table '{target_table}' ({row_count} rows)"
            )

    def on_deliverable_written(self, path: str, row_count: int) -> None:
        click.echo(f"  [ok] {path} ({row_count} rows)")


# ---------------------------------------------------------------------------
# Prescriptive error output formatter (CLAUDE.md design principle)
# ---------------------------------------------------------------------------
# Append this to the end of prompts.py.
# Used by vp errors and vp status to produce "N records failed because X.
# To fix: run Y." output.

from dataclasses import dataclass, field


@dataclass
class ErrorGroup:
    """A group of errors sharing the same cause."""
    cause: str
    count: int
    explanation: str
    fix_commands: list[str] = field(default_factory=list)


# Fix commands keyed by (stage, check_name).
# stage is "source" or "report". check_name is from source_block.check_name
# or validation_block.check_name.
_SOURCE_FIXES: dict[str, tuple[str, list[str]]] = {
    "type_conflict": (
        "values don't match the declared column type",
        [
            "vp errors source export {name} --open  →  fix values  →  vp errors source retry {name}",
            "vp edit column-type  →  vp ingest",
        ],
    ),
    "duplicate_conflict": (
        "incoming row conflicts with an existing record",
        [
            "vp errors source export {name} --open  →  fix values  →  vp errors source retry {name}",
            "vp errors source clear {name}",
        ],
    ),
}

_REPORT_FIXES: dict[str, tuple[str, list[str]]] = {
    "__check_failure__": (
        "check returned failing rows",
        [
            "vp errors report export {name} --open  →  fix values  →  vp errors report retry {name}",
            "if the check logic is wrong (not the data): edit the function in your checks file  →  vp validate --full",
            "vp errors report clear {name}",
        ],
    ),
    "__transform_type_mismatch__": (
        "transform output type doesn't match the column type",
        [
            "vp edit column-type  →  vp validate --full",
            "if the transform logic is wrong: edit the function in your checks file  →  vp validate --full",
        ],
    ),
}


def build_error_groups(
    rows: list[tuple[str, int]],
    stage: str,
    name: str | None = None,
) -> list[ErrorGroup]:
    """Build ErrorGroup list from (check_name, count) pairs.

    Args:
        rows: list of (check_name, count) tuples from a GROUP BY query.
        stage: "source" or "report".
        name: optional table/report name to interpolate into fix commands.
    """
    fixes = _SOURCE_FIXES if stage == "source" else _REPORT_FIXES
    label = name or "<name>"
    groups = []
    for check_name, count in rows:
        if check_name in fixes:
            explanation, cmds = fixes[check_name]
        elif stage == "report":
            explanation, cmds = fixes["__check_failure__"]
        else:
            explanation = "unknown error type"
            cmds = []

        groups.append(ErrorGroup(
            cause=check_name,
            count=count,
            explanation=explanation,
            fix_commands=[c.format(name=label) for c in cmds],
        ))
    return groups


def format_error_groups(groups: list[ErrorGroup], indent: int = 4) -> None:
    """Print prescriptive error output to terminal.

    Pattern: cause: N row(s) — explanation
             Fix: exact commands
    """
    pad = " " * indent
    for g in groups:
        click.echo(f"{pad}{g.cause}: {g.count} row(s) — {g.explanation}")
        for i, cmd in enumerate(g.fix_commands):
            prefix = "Fix:" if i == 0 else "  or:"
            click.echo(f"{pad}  {prefix} {cmd}")


# ---------------------------------------------------------------------------
# Append to prompts.py — after the previous ErrorGroup / format_error_groups block.
# Display functions for vp status and vp errors commands.
# ---------------------------------------------------------------------------


def print_error_overview(overview) -> None:
    """Display bare `vp errors` summary — counts from both stages.

    Args:
        overview: ErrorOverview from query_error_overview.
    """
    has_source = overview.source_count > 0 or overview.file_failure_count > 0
    has_report = overview.report_count > 0

    if not has_source and not has_report:
        click.echo("\n  No pipeline errors — all clear.")
        return

    click.echo("\n  Pipeline Errors")
    click.echo(f"  {'─' * 50}")

    if has_source:
        parts = []
        if overview.source_count:
            parts.append(f"{overview.source_count} blocked row(s)")
        if overview.file_failure_count:
            parts.append(f"{overview.file_failure_count} file failure(s)")
        click.echo(f"\n  Source:  {' + '.join(parts)}")
        click.echo(f"    Run: vp errors source")
    else:
        click.echo(f"\n  Source:  no errors")

    if has_report:
        click.echo(
            f"\n  Report:  {overview.report_count} validation failure(s) across "
            f"{overview.report_name_count} report(s)"
        )
        click.echo(f"    Run: vp errors report")
    else:
        click.echo(f"\n  Report:  no errors")

    click.echo()


def print_prescriptive_errors(
    stage: str,
    name: str,
    by_scope: dict[str, list[tuple[str, int]]],
) -> None:
    """Display detailed errors for ONE source or report with prescriptive fixes.

    Only called with a name — list views use print_source_error_list / print_report_error_list.

    Args:
        stage: "source" or "report".
        name: table_name (source) or report_name (report).
        by_scope: dict from query_error_groups — scope_name → [(check_name, count)].
    """
    if not by_scope:
        return

    total = sum(c for checks in by_scope.values() for _, c in checks)

    click.echo(f"\n  Row-level errors ({total} rows)")
    for scope_name, check_rows in by_scope.items():
        groups = build_error_groups(check_rows, stage, name=scope_name)
        for g in groups:
            click.echo(f"    {g.cause}: {g.count} row(s) — {g.explanation}")
            for i, cmd in enumerate(g.fix_commands):
                prefix = "Fix:" if i == 0 else "  or:"
                click.echo(f"      {prefix} {cmd}")


def print_source_error_list(summaries) -> None:
    """One-line-per-source list for `vp errors source` (no name).

    Args:
        summaries: list of SourceErrorSummary from query_source_error_summary.
    """
    if not summaries:
        click.echo("\n  No source errors — all clear.")
        return

    click.echo("\n  Source Errors")
    click.echo(f"  {'─' * 50}")
    click.echo(f"  {'Table':<25} {'Blocked':>10} {'File fails':>12}")
    click.echo(f"  {'─' * 50}")

    for s in summaries:
        blocked = str(s.blocked_count) if s.blocked_count else "—"
        file_fails = str(s.file_failure_count) if s.file_failure_count else "—"
        click.echo(f"  {s.table_name:<25} {blocked:>10} {file_fails:>12}")

    click.echo(f"\n  Run: vp errors source <name>  for details")
    click.echo()


def print_report_error_list(summaries) -> None:
    """One-line-per-report list for `vp errors report` (no name).

    Args:
        summaries: list of ReportErrorSummary from query_report_error_summary.
    """
    if not summaries:
        click.echo("\n  No report errors — all clear.")
        return

    click.echo("\n  Report Errors")
    click.echo(f"  {'─' * 50}")
    click.echo(f"  {'Report':<30} {'Failures':>10}")
    click.echo(f"  {'─' * 50}")

    for s in summaries:
        click.echo(f"  {s.report_name:<30} {s.failure_count:>10}")

    click.echo(f"\n  Run: vp errors report <name>  for details")
    click.echo()


def print_file_failures(file_failures, detail: bool = False) -> None:
    """Display file-level ingest failures from ingest_state.

    Groups by error message so duplicate reasons are shown once with
    a list of affected filenames.

    Args:
        file_failures: list of FileFailure from query_file_failures.
        detail: True for detail view (with name), False unused here.
    """
    if not file_failures:
        return

    from collections import defaultdict

    # Group by message — same error across files shown once
    by_message: dict[str, list[str]] = defaultdict(list)
    for f in file_failures:
        by_message[f.message].append(f.filename)

    click.echo(f"\n  File-level failures ({len(file_failures)} file(s))")
    for message, filenames in by_message.items():
        if len(filenames) == 1:
            click.echo(f"    {filenames[0]}: {message}")
        else:
            click.echo(f"    {message} ({len(filenames)} files)")
            for fn in filenames:
                click.echo(f"      {fn}")
    click.echo(f"    Fix: vp edit column-type  or fix the file, then: vp ingest")


def print_health_summary(health, deliverable_names: list[str]) -> None:
    """Display bare `vp status` health overview with prescriptive next steps.

    Args:
        health: PipelineHealth from query_pipeline_health.
        deliverable_names: from DeliverableConfig (not DB-derived).
    """
    click.echo("\nPipeline Status\n")

    # Sources
    if health.source_tables:
        err = f"    {health.source_error_count} error(s)" if health.source_error_count else ""
        click.echo(f"  Sources:       {len(health.source_tables)} ingested{err}")
    else:
        click.echo("  Sources:       none ingested yet")

    # Reports
    if health.report_names:
        fail = f"    {health.validation_failure_count} failure(s)" if health.validation_failure_count else ""
        click.echo(f"  Reports:       {len(health.report_names)} validated{fail}")
    else:
        click.echo("  Reports:       none validated yet")

    # Deliverables
    if deliverable_names:
        click.echo(f"  Deliverables:  {len(deliverable_names)} configured")
    else:
        click.echo("  Deliverables:  none configured")

    # Prescriptive next steps
    steps = []
    if not health.source_tables:
        steps.append("vp new source  →  vp ingest")
    if health.source_error_count:
        steps.append(f"vp errors source — {health.source_error_count} error(s) need resolution")
    if health.source_tables and not health.report_names:
        steps.append("vp new report  →  vp validate")
    if health.validation_failure_count:
        steps.append(f"vp errors report — {health.validation_failure_count} failure(s) to review")
    if health.report_names and not deliverable_names:
        steps.append("vp new deliverable  →  vp deliver <n>")

    if steps:
        click.echo("\n  Next steps:")
        for s in steps:
            click.echo(f"    {s}")
    else:
        click.echo("\n  All clear — ready to deliver.")


def print_source_list(statuses) -> None:
    """Display one-line summary per source table.

    Args:
        statuses: list of SourceStatus from query_source_statuses.
    """
    if not statuses:
        click.echo("No sources ingested yet. Run: vp ingest")
        return

    click.echo("\nSources\n")
    for s in statuses:
        err = f"{s.error_count} error(s)" if s.error_count else ""
        last = s.last_ingest or "—"
        click.echo(f"{s.table_name:<25} {s.total_rows:>8} rows last: {last}{err}")


def print_source_detail(detail) -> None:
    """Display detailed status for one source table.

    Args:
        detail: SourceDetail from query_source_detail.
    """
    click.echo(f"\n  Source: {detail.name}")
    click.echo(f"  {'─' * 40}")

    if detail.row_count is not None:
        click.echo(f"  Rows:     {detail.row_count:,}")
    else:
        click.echo(f"  Rows:     table not found")
        if detail.last_failure_message:
            click.echo(f"  Reason:   {detail.last_failure_message}")
            click.echo(f"  Fix:      fix the issue and run: vp ingest")

    if detail.error_count:
        click.echo(f"  Errors:   {detail.error_count}")
        click.echo(f"  Fix:      vp errors source {detail.name}")
    else:
        click.echo(f"  Errors:   none")

    if detail.history:
        click.echo(f"\n  Ingest history:")
        click.echo(f"  {'Date':<12} {'File':<35} {'Status':<10} {'Rows':>6}")
        click.echo(f"  {'─' * 65}")
        for h in detail.history:
            ts = h["ingested_at"] or "—"
            rows = str(h["rows"]) if h["rows"] is not None else "—"
            status = h["status"] or "—"
            click.echo(f"  {ts:<12} {h['filename']:<35} {status:<10} {rows:>6}")
    click.echo()


def print_report_list(statuses) -> None:
    """Display one-line summary per report.

    Args:
        statuses: list of ReportStatus from query_report_statuses.
    """
    if not statuses:
        click.echo("No reports validated yet. Run: vp validate")
        return

    click.echo("\nReports\n")
    for s in statuses:
        fail = f"{s.failure_count} failure(s)" if s.failure_count else ""
        last = s.last_validated or "—"
        click.echo(f"{s.report_name:<25} {s.record_count:>8} records last: {last}{fail}")


def print_report_detail(detail) -> None:
    """Display detailed status for one report.

    Args:
        detail: ReportDetail from query_report_detail.
    """
    click.echo(f"\n  Report: {detail.name}")
    click.echo(f"  {'─' * 40}")

    if detail.row_count is not None:
        click.echo(f"  Rows:       {detail.row_count:,}")
    else:
        click.echo(f"  Rows:       table not found (run vp validate)")

    click.echo(f"  Validated:  {detail.last_validated or '—'}")

    if detail.failure_count:
        click.echo(f"  Failures:   {detail.failure_count}")
        click.echo(f"  Fix:        vp errors report {detail.name}")
    else:
        click.echo(f"  Failures:   none")

    if detail.checks:
        click.echo(f"\n  Check results:")
        click.echo(f"  {'Check':<30} {'Status':<10} {'Records':>8}")
        click.echo(f"  {'─' * 50}")
        for check_name, status, cnt in detail.checks:
            click.echo(f"  {check_name:<30} {status:<10} {cnt:>8}")
    click.echo()
