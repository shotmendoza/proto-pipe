from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path

import click


# ---------------------------------------------------------------------------
# New command: vp check-func
# ---------------------------------------------------------------------------
@click.command("funcs")
@click.option(
    "--custom-checks",
    default=None,
    help="Path to custom checks module (overrides pipeline.yaml).",
)
def funcs_cmd(custom_checks):
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
    from proto_pipe.checks.helpers import DECORATED_CHECKS
    from proto_pipe.io.config import load_settings
    from proto_pipe.checks.registry import CheckRegistry as _TempRegistry

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
            DECORATED_CHECKS.clear()
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
                if not DECORATED_CHECKS:
                    custom_load_error = (
                        f"Loaded '{module_path}' but found no @custom_check decorated functions. "
                        f"Add @custom_check to each function you want registered."
                    )
                else:
                    custom_names = set(DECORATED_CHECKS.keys())
                    for name, (func, kind) in DECORATED_CHECKS.items():
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
            "\n  Checks marked ✗ are silently skipped during 'vp new report'."
            "\n  Fix the issues above and run 'vp funcs' again to confirm."
        )


def funcs_commands(cli: click.Group) -> None:
    cli.add_command(funcs_cmd)
