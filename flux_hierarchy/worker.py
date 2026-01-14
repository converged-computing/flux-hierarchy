import json
import shutil
import sys

import flux_hierarchy.utils as utils
from flux_hierarchy.hierarchy import FluxWorkerHierarchy


def main():
    payload = json.loads(sys.argv[1])
    sockets = payload["sockets"]
    commands = payload["commands"]
    result_file = payload["result_file"]
    count = payload["count"]
    clean_env = payload.get("clean_env") is True
    clones = payload.get("clones") is True

    # Instantiate the Hierarchy Worker
    # This handles connecting to the local sockets (local://) provided in the payload
    hier = FluxWorkerHierarchy(uris=sockets, clean_env=clean_env)

    print(f"Worker starting: {count} jobs on {len(sockets)} brokers...")

    if clones:
        # Throughput mode: Run the same command 'count' times across available brokers
        # commands[0] is expected to be the single command list, e.g. ['sleep', '1']
        results = hier.throughput(commands[0], count)
    else:
        # Distinct mode: Run the specific list of commands provided
        results = hier.submit_jobs(commands)

    # Atomic Write: Save first as lock file to signal completion phase
    lock_file = result_file + ".lock"

    # Write the JSON results
    utils.write_json(results, lock_file)

    # Rename to final result file (atomic commit)
    shutil.move(lock_file, result_file)
    print(f"Worker finished. Results written to {result_file}")


if __name__ == "__main__":
    main()
