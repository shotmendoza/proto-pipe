"""CLI prompts — SourceConfigPrompter.

Owns all questionary interactions for configuring a data source.
No DB connections, no pandas beyond reading the sample for display,
no business logic — pure UI.

Both `vp source new` and `vp source edit` instantiate this class and
call its methods to collect user input.
"""
from __future__ import annotations

import click
import questionary

# Common DuckDB types shown in all type selection prompts
_DUCKDB_TYPES = ["VARCHAR", "DOUBLE", "BIGINT", "BOOLEAN", "DATE", "TIMESTAMPTZ"]

# Common date format candidates shown when DATE or TIMESTAMPTZ is selected
_DATE_FORMATS = [
    ("%Y-%m-%d",  "2026-01-06"),
    ("%m/%d/%Y",  "01/06/2026"),
    ("%m-%d-%y",  "01-06-26"),
    ("%d/%m/%Y",  "06/01/2026"),
    ("%Y/%m/%d",  "2026/01/06"),
    ("%m/%d/%y",  "01/06/26"),
]


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


class SourceConfigPrompter:
    """Collects user input for creating or editing a source configuration.

    :param sample_df:       DataFrame loaded from the sample file, or None if
                            no file was selected.
    :param registry_hints:  {column: {source_name: declared_type}} from
                            column_type_registry — used to show existing
                            type declarations and conflicts.
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
        self._file_cols: list[str] = (
            [c for c in sample_df.columns if not c.startswith("_")]
            if sample_df is not None
            else []
        )

    # ---------------------------------------------------------------------------
    # Source identity
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

    # ---------------------------------------------------------------------------
    # Primary key
    # ---------------------------------------------------------------------------

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

    # ---------------------------------------------------------------------------
    # Duplicate handling + timestamp
    # ---------------------------------------------------------------------------

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

    # ---------------------------------------------------------------------------
    # Column types
    # ---------------------------------------------------------------------------

    def prompt_column_types(self) -> dict[str, str]:
        """Show inferred type summary and let the user confirm or override.

        Returns {column_name: declared_type} for all file columns.
        Returns {} if no sample file was loaded.
        """
        if self._sample is None or not self._file_cols:
            return {}

        # Build inferred types
        inferred_types = {col: _infer_duckdb_type(self._sample[col]) for col in self._file_cols}

        # Registry wins when all sources agree on one type
        working_types: dict[str, str] = {}
        for col in self._file_cols:
            hints = self._registry_hints.get(col, {})
            unique_types = set(hints.values())
            if len(unique_types) == 1:
                working_types[col] = next(iter(unique_types))
            else:
                working_types[col] = inferred_types[col]

        # Show summary with registry notes
        click.echo("\n  Inferred column types:")
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

            # Show sample value alongside column name
            sample_val = self._sample[col].dropna().iloc[0] if not self._sample[col].dropna().empty else "—"
            click.echo(f"    {col:<28} {dtype:<14} sample: {sample_val}{note}")

        looks_right = questionary.confirm(
            "\n  Do these look right?",
            default=True,
        ).ask()
        if looks_right is None:
            return {}

        if not looks_right:
            to_fix = questionary.checkbox(
                "Select the columns to change:",
                choices=[
                    questionary.Choice(
                        f"{col:<28} {working_types[col]}",
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
                    conflict_parts = ", ".join(f"{t} in '{s}'" for s, t in hints.items())
                    click.echo(f"  ⚠ '{col}' has conflicting registry types: {conflict_parts}")

                chosen = questionary.select(
                    f"  '{col}' (currently {current}):",
                    choices=[
                        questionary.Choice(
                            f"{t}{' (current)' if t == current else ''}",
                            value=t,
                        )
                        for t in _DUCKDB_TYPES
                    ],
                    default=current,
                ).ask()
                if chosen is None:
                    return {}
                working_types[col] = chosen

                # If a date type was chosen, prompt for the format
                if working_types[col] in ("DATE", "TIMESTAMPTZ"):
                    fmt = self.prompt_date_format(col)
                    if fmt:
                        # Store format alongside type using | separator
                        # Format is used in _apply_declared_types
                        working_types[col] = f"{working_types[col]}|{fmt}"

        return working_types

    def prompt_date_format(self, col: str) -> str | None:
        """Prompt for date format string when a date type is selected.

        Shows common format candidates with a sample value and marks which
        ones successfully parse it.

        Returns the strptime format string, or None if the user skips.
        """
        sample_val = None
        if self._sample is not None and col in self._sample.columns:
            non_null = self._sample[col].dropna()
            if not non_null.empty:
                sample_val = str(non_null.iloc[0])

        click.echo(f"\n  Date format for '{col}':")
        if sample_val:
            click.echo(f"  Sample value: {sample_val}\n")

        choices = []
        for fmt, example in _DATE_FORMATS:
            matches = _try_strptime(sample_val, fmt) if sample_val else False
            label = f"{fmt:<16} e.g. {example}"
            if matches:
                label += "  ✓ matches sample"
            choices.append(questionary.Choice(label, value=fmt))
        choices.append(questionary.Choice("Enter custom format", value="__custom__"))
        choices.append(questionary.Choice("Skip — store as VARCHAR instead", value="__skip__"))

        chosen = questionary.select(
            f"  Select format for '{col}':",
            choices=choices,
        ).ask()

        if chosen is None or chosen == "__skip__":
            return None
        if chosen == "__custom__":
            custom = questionary.text(
                "Enter strptime format string (e.g. %Y-%m-%d):"
            ).ask()
            return custom.strip() if custom else None
        return chosen
