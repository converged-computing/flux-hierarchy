#!/usr/bin/env python3

import time

from flux_hierarchy import FluxHierarchy
from flux_hierarchy.logger import LogColors


def main(args, _):

    # Instantiate, build, and connect to the Flux Hierarchy!
    hierarchy = FluxHierarchy(args.config, args.outdir, clean_env=not args.keep_env)
    hierarchy.start(interactive=False)

    # Default to true if not set.
    if not args.execute:
        args.execute = ["true"]

    # Run the throughput test using the specialized 'throughput' method
    time0 = time.time()
    results = hierarchy.throughput(args.execute, args.njobs, args.local)

    # Keep the tree running? And if so, cleanup asset directories?
    if not args.keep:
        hierarchy.stop(cleanup=not args.skip_cleanup)

    if not results:
        print(f"{LogColors.RED}No jobs were tracked. Cannot calculate throughput.{LogColors.ENDC}")
        return

    # Earliest start, latest end, total time to submit
    start_time = min(results["start_times"])
    end_time = max(results["end_times"])

    # This is JUST submit
    script_runtime = time.time() - time0

    # This includes the job running - submit_t through clenaup_t
    job_runtime = end_time - start_time
    jps = args.njobs / job_runtime if job_runtime > 0 else float("inf")
    jpsb = args.njobs / script_runtime if script_runtime > 0 else float("inf")

    print(f"\n--- Throughput Results ---")
    print(f"number of jobs: {args.njobs} (on {len(hierarchy.handles)} workers)")
    print(f"script runtime: {script_runtime:<6.3f}s")
    print(f"   job runtime: {job_runtime:<6.3f}s")
    print(f"    throughput: {jps:<.1f} job/s (script: {jpsb:5.1f} job/s)")
