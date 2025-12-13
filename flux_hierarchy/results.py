import glob
import os

import flux_hierarchy.utils as utils


def combine_results(results_dir):
    """
    Read and synthesize total results.
    """
    results = {
        "total_submitted": 0,
        "start_times": [],
        "end_times": [],
        "submit_times": [],
        "submit_end_times": [],
    }
    files = glob.glob(results_dir + os.sep + "*.json")
    for result_file in files:
        result = utils.read_json(result_file)
        for key, values in result.items():
            # We already parsed this
            if key == "results_per_worker":
                continue
            results[key] += values
    return results


def summarize_results(results, count):
    total_submitted = sum([x[0] for x in results])
    print(f"\n=> Summary:")
    print(f"  - Approximate submissions per worker: {results[0][0]}")
    print(f"  - Total jobs submitted: {total_submitted} / {count}")

    # Now, build the final data structures from the collected info
    start_times = []
    end_times = []

    # The job_info_dict is now fully populated...
    for result in results:
        for info in result[3].values():
            start_times.append(info["submit"]["timestamp"])
            end_times.append(info["clean"]["timestamp"])

    assert len(start_times) == count
    assert len(end_times) == count

    # Reconstruct the exact return signature you had
    return {
        "total_submitted": total_submitted,
        "results_per_worker": results,
        "start_times": start_times,
        "end_times": end_times,
        "submit_times": [res[1] for res in results],
        "submit_end_times": [res[2] for res in results],
    }
