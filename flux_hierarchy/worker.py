import json
import shutil
import sys

import flux_hierarchy.utils as utils
from flux_hierarchy.hierarchy import FluxWorkerHierarchy
from flux_hierarchy.results import summarize_results


def main():
    # Read payload from first argument
    payload = json.loads(sys.argv[1])
    sockets = payload["sockets"]
    commands = payload["commands"]
    result_file = payload["result_file"]
    count = payload["count"]
    clean_env = payload.get("clean_env") is True

    # Submit the same command many times?
    clones = payload.get("clones") is True

    # Use same classes provided by flux_hierarchy!
    hier = FluxWorkerHierarchy(uris=sockets, clean_env=clean_env)

    # It's assumed the worker
    if clones:
        results = hier.throughput(commands[0], count)
    else:
        results = hier.submit_jobs(commands)

    # Save first as lock file.
    lock_file = result_file + ".lock"

    # Save results to output directory where launcher expecting to find them.
    # This is imperfect but will work for testing on a shared fs.
    utils.write_json(results, lock_file)

    # Rename to results file
    shutil.move(lock_file, result_file)


if __name__ == "__main__":
    main()
