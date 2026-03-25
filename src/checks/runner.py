from concurrent.futures import ThreadPoolExecutor, as_completed

from src.registry.base import CheckRegistry


# ---------------------------------------------------------------------------
# Check execution
# ---------------------------------------------------------------------------
def run_check_safe(
        registry: CheckRegistry,
        check_name: str,
        context: dict
) -> dict:
    """Run a single check, catching exceptions so one failure doesn't halt the report.

    :param registry: a registry instance that will run the checks in parallel
    :param check_name: Name of the check that is being run
    :param context: A dictionary containing a `df` key, with a DataFrame value

    """
    try:
        result = registry.run(check_name, context)
        return {"status": "passed", "result": result}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


def run_checks(
    check_names: list[str],
    registry: CheckRegistry,
    context: dict,
    parallel: bool = False,
) -> dict:
    """manager object that determines whether the checks should be run in parallel or thread safe.

    :param check_names: name of the checks in the registry
    :param registry: where all the checks are logged
    :param context: a dictionary containing a `df` key, with a DataFrame value
    :param parallel: whether to run the checks in parallel
    :return: the results of the checks
    """
    if not parallel:
        return {
            name: run_check_safe(registry, name, context)
            for name in check_names
        }

    results = {}
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(run_check_safe, registry, name, context): name
            for name in check_names
        }
        for future in as_completed(futures):
            name = futures[future]
            results[name] = future.result()
    return results
