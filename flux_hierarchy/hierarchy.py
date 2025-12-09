import json
import multiprocessing
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time

import flux_hierarchy.utils as utils
from flux_hierarchy.logger import LogColors

try:
    import flux
    import flux.job
    from flux.progress import Bottombar
except ImportError:
    flux = None


class FluxHierarchy:
    """
    A FluxHierarchy allows for immediate or dynamic submission of
    jobs to a hierarchy of Flux instances. To start, we calculate
    instance sizes based on the resources given at the top level.

    # TODO: should be able to read in directory of active sockets.
    # TODO: allow different job shapes / specs.
    """

    def __init__(self, config_path, outdir=None, prefix=None, keep_env=False):
        """
        Create an output directory for jobspecs and submit files.
        """
        # We will store a lookup of uris
        self.config = utils.read_yaml(config_path)
        self.prefix = prefix or ""

        # You can't handle me right now.
        self.uris = {}
        self.handles = {}
        self.init_structure(outdir)
        self.groups = {g["name"]: g for g in self.config["groups"]}
        self.clean_env = not keep_env

        # Still thinking about this one. I think it should be possible
        # to define an entire tree, but then only create a subgraph of it
        # This is the entrypoint.
        self.entrypoint = self.config["entrypoint"]

    def init_structure(self, outdir):
        """
        Create output directories.
        """
        # Trying to make as short as possible
        self.outdir = outdir or tempfile.mkdtemp(prefix="fh-")
        self.socket_dir = os.path.join(self.outdir, "sock")
        self.uri_dir = os.path.join(os.getcwd(), "uris")
        if os.path.exists(self.uri_dir):
            shutil.rmtree(self.uri_dir)
        self.logs_dir = os.path.join(self.outdir, "logs")
        for path in self.socket_dir, self.logs_dir, self.uri_dir:
            os.makedirs(path, exist_ok=True)

    @property
    def resources(self):
        return self.config["resources"]

    def pprint(self, message):
        """
        Print a pretty section!
        """
        print(f"=> {LogColors.OKCYAN}{message}{LogColors.ENDC}", end="")

    def check(self):
        """
        Check to make sure we have imported flux.
        """
        if not flux:
            raise ValueError("Cannot import flux, which is needed here.")

    def view(self):
        """
        View the shape of a config.
        """
        self.pprint(f"\n🌿 Leaf Broker Workers...")
        print(json.dumps(self.uris, indent=2))
        self.print_tree()

    def stage(self):
        """
        Stage the entire temporary directory across all nodes before start.
        """
        name = os.path.basename(self.outdir)
        cmd = ["flux", "archive", "create", "-C", self.outdir, "--name", name, "."]
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        cmd = ["flux", "exec", "-r", "all", "-x", "0", "mkdir", "-p", self.outdir]
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        cmd = [
            "flux",
            "exec",
            "-r",
            "all",
            "-x",
            "0",
            "flux",
            "archive",
            "extract",
            "-C",
            self.outdir,
            "--name",
            name,
            "--overwrite",
        ]
        subprocess.run(cmd, capture_output=True, text=True, check=True)

    def start(self, interactive=True):
        """
        Start a flux hierarchy of a specific size. Currently, let's not submit
        commands entire the entire thing is created, and we will call this a
        startup cost.
        """
        self.check()

        # Provide the first instance level (0)
        # If it's a lead group, we need to collect the URI.
        self.pprint(f"🌲 Generating Flux Hierarchy...\n")
        filepath, is_leaf_group = self.generate(self.entrypoint, "0")

        # We need to stage the scripts across all nodes
        self.stage()

        # This launches our entrypoint to create nested hierarchy
        self.pprint(f"\n🚗 Starting...\n")
        cmd = ["flux", "job", "submit", "--flags=waitable", filepath]
        print(" ".join(cmd))
        jobid = utils.run_command(cmd, check_output=True)["message"].strip()
        if is_leaf_group:
            self.save_uri(jobid, "0")

        # Show the brokers (with node addresses)
        self.pprint(f"\n🌿 Leaf Broker Workers...")
        print(json.dumps(self.uris, indent=2))
        self.print_tree()

        # Load URIs - this converts local:// into ssh:// with hostname
        self.load_uris()
        print(json.dumps(self.uris, indent=2))

        # Give the user an interactive mode
        self.connect()
        if interactive:
            self.interactive()

        # Assume we want to return URIs to interact with
        return self.uris

    def save_uri(self, jobid, uri_id):
        """
        Derive a URI for a job id and save based on the identifier
        to the URI directory.
        """
        uri = utils.run_command(["flux", "uri", "--wait", jobid], check_output=True)[
            "message"
        ].strip()
        utils.write_file(uri, os.path.join(self.uri_dir, uri_id))

    def load_uris(self):
        """
        Load hierarchy uris.
        """
        # Wait until uris are generated
        while len(os.listdir(self.uri_dir)) < len(self.uris):
            time.sleep(1)

        uris = {}
        print()
        for name, _ in self.uris.items():
            uri_path = os.path.join(self.uri_dir, name)
            while not os.path.exists(uri_path):
                self.pprint(f"[Waiting] {name}")
                time.sleep(1)
            uri = utils.read_file(uri_path).strip()
            if not uri:
                raise ValueError(f"URI for {name} was empty.")
            uris[name] = uri
        print()
        self.uris = uris

    def connect(self):
        """
        Connect to all leaf broker URIs and store the handles.
        """
        if not self.uris:
            return
        self.pprint(f"Waiting for {len(self.uris)} leaf brokers...\n")
        for name, uri in self.uris.items():
            self.handles[name] = flux.Flux(uri)
            self.handles[name].uri = uri
        self.pprint(f"Connected!\n")

    def interactive(self):
        """
        Interactive mode to submit.
        """
        print(f"\n{LogColors.BOLD}Dropping into an interactive IPython shell.{LogColors.ENDC}")
        print("The 'self' object is the FluxHierarchy instance.")
        print(
            f"Throughput test: {LogColors.OKCYAN}results = self.throughput(['sleep', '1'], 16){LogColors.ENDC}"
        )
        print(
            f"    Submit jobs: {LogColors.OKCYAN}results = self.submit_jobs([['hostname'], ['date']]){LogColors.ENDC}"
        )
        import IPython

        IPython.embed()

    def generate(self, group_name, instance_path):
        """
        Recursively generate and save a self-contained jobspec for a group.

        We need to return if a left node was generated, which would populate
        URIs. If the generated script is a leaf node, we need to collect a uri
        from the job there (e.g., the top level.).
        """
        print(f"- Generating: {group_name} (instance: {instance_path})")
        group = self.groups[group_name]

        # A group is a leaf if it does NOT have a launch section.
        is_leaf_group = "launch" not in group

        # The resources label points to resources for the group
        label = group["resources"]
        log_path = os.path.join(self.logs_dir, group_name + ".out")
        jobspec_filename = os.path.abspath(
            os.path.join(self.outdir, f"jobspec-{group_name}-{instance_path}.json")
        )
        # Make the socket paths / uris in same root, regardless of intended interaction
        socket_path = os.path.abspath(os.path.join(self.socket_dir, f"{instance_path}.sock"))
        uri_string = f"local://{socket_path}"

        # A leaf broker runs a broker that we can submit to
        if is_leaf_group:
            self.uris[instance_path] = uri_string
            command = ["flux", "broker", "-S", f"local-uri={uri_string}", "sleep", "infinity"]

        # If we get here, we are generating an intermediate node.
        # Note that I tried first specifying cores/tasks, but it works better
        # to ask for exclusive.
        else:
            # Recursively generate all child files and collect their paths
            child_paths = {}
            for task in group["launch"]:
                name = task["group"]
                count = task.get("count") or 1

                for i in range(count):
                    task_path = f"{instance_path}-{i}"
                    child_path, _ = self.generate(name, task_path)
                    child_paths[task_path] = child_path

            # Use flux trick to generate inner file for broker to execute
            script_path = os.path.abspath(
                os.path.join(self.outdir, f"inner-script-{group_name}-{instance_path}.sh")
            )
            script = self.generate_script(child_paths)
            utils.write_file(script, script_path, executable=True)

            # 3. Get the jobspec for this intermediate broker
            command = ["flux", "broker", "-S", f"local-uri={uri_string}", script_path]

        # Save the jobspec for the leaf or intermediate node.
        jobspec = get_jobspec_from_dry_run(
            command, self.resources[label], log_path, clean_env=self.clean_env
        )
        utils.write_json(jobspec, jobspec_filename)
        return jobspec_filename, is_leaf_group

    def generate_script(self, child_paths):
        """
        Generate an intermediate worker script.

        Jinja2 would be better here, but don't want to add dependency.
        """
        script = "#!/bin/bash\n"
        script += "set -euo pipefail\n\n"

        # Generate the child jobs
        for task_name, child_path in child_paths.items():
            script += f"flux job submit --flags=waitable {child_path}\n"
            # Wait for URIs to be ready...
            script += f"flux uri --wait $(flux job last)\n"
            # Add the uri to the URIs directory. This isn't great, will work for now
            script += "job_host=$(flux hostlist --expand $(flux jobs -o '{nodelist}' $(flux job last) | sed -n '2p') | cut -d ' ' -f 1)\n"
            script += f'echo "ssh://${{job_host}}{self.socket_dir}/{task_name}.sock" > {self.uri_dir}/{task_name}\n'
            # script += f"flux uri --wait $(flux job last) > {self.uri_dir}/{task_name}\n"
        script += "\nflux job wait --all\n"
        return script

    @property
    def kvs_path(self):
        """
        kvs path for a named job. Not currently used.
        """
        prefix = self.prefix or "hierarchy"
        return f"guest.handles.{prefix}"

    def __exit__(self):
        """
        Cleanup: Recursively delete the session directory in the KVS.
        """
        # TODO some kind of cleanup, if desired.
        pass
        print(f"[Cleanup] Removing KVS session: {self.kvs_path}")

    def print_tree(self):
        """
        Print a pretty ASCII tree of the hierarchy defined in the config.
        THERE IS NO OTHER WAY.
        """
        self._print_tree_recursive(self.entrypoint)

    def _print_tree_recursive(self, group_name, prefix="", is_last=True):
        """
        A recursive helper function to print the hierarchy tree.
        """
        group = self.groups[group_name]
        resource_key = group["resources"]
        resource_def = self.resources[resource_key]

        # Make a resource string to show structure
        parts = []
        if "nodes" in resource_def:
            parts.append(f"Nodes: {resource_def['nodes']}")
        if "cores" in resource_def:
            parts.append(f"Cores: {resource_def['cores']}")
        if "count" in resource_def:
            parts.append(f"Tasks: {resource_def['count']}")
        result = f"[{', '.join(parts)}]"

        # Determine the branch character and color for leaf/intermediate nodes
        is_leaf = "launch" not in group
        branch_char = "└── " if is_last else "├── "

        # Make the leaves green!
        name_color = LogColors.OKGREEN if is_leaf else LogColors.OKBLUE

        # The root node has no prefix
        if prefix == "":
            print(f"{name_color}{LogColors.BOLD}{group_name}{LogColors.ENDC} {result}")
        else:
            print(f"{prefix}{branch_char}{name_color}{group_name}{LogColors.ENDC} {result}")

        # If it's an intermediate node, recurse
        if not is_leaf:

            # Create a flat list of all children to launch
            children_to_launch = []
            for task in group.get("launch", []):
                count = task.get("count", 1)
                children_to_launch.extend([task["group"]] * count)

            # Recurse, adjusting the prefix for the next level
            num_children = len(children_to_launch)
            for i, child_name in enumerate(children_to_launch):
                is_child_last = i == num_children - 1
                new_prefix = prefix + ("    " if is_last else "│   ")
                self._print_tree_recursive(child_name, new_prefix, is_child_last)

    def submit_jobs(self, commands, equivalent=True):
        """
        Submits a list of jobs using a multiprocessing pool where each
        worker executes `flux bulksubmit`. If equivalent is True, all
        the jobspecs are the same (e.g., throughput use case).
        """
        self.pprint(f"Starting submission of {len(commands)} jobs...")

        # Instantiate and use the MultiprocessBulkRunner
        runner = MultiprocessBulkRunner(list(self.handles.values()))
        if equivalent:
            results = runner.run_clones(commands[0], count=len(commands))
        else:
            results = runner.run(commands, equivalent=True)

        self.pprint("Submission process complete.\n")
        return results

    def throughput(self, command, count=100):
        """
        A specialized function to test throughput by submitting one command many times.
        """
        print(f"Preparing throughput test for command: {' '.join(command)}")
        commands_list = [command for _ in range(count)]
        return self.submit_jobs(commands_list, equivalent=True)


def get_jobspec_from_dry_run(command, resources, log_path=None, clean_env=True):
    """
    Use flux to generate a json jobspec with flux submit --dryrun
    """
    cores = resources.get("cores")
    nodes = resources.get("nodes")
    tasks = resources.get("count")
    exclusive = resources.get("exclusive") in ["true", "yes", "True", True]

    cmd = ["flux", "submit", "--dry-run"]
    if log_path is not None:
        cmd += ["--out", log_path, "--err", log_path]
    if nodes is not None:
        cmd += ["-N", str(nodes)]
    if tasks is not None:
        cmd += ["-n", str(tasks)]
    if cores is not None:
        cmd += ["--cores", str(cores)]
    if exclusive:
        cmd += ["--exclusive"]
    cmd += command
    process = subprocess.run(cmd, capture_output=True, text=True, check=True)
    js = json.loads(process.stdout)

    # Clean the environment
    if clean_env:
        keepers = {}
        keep_names = ["PATH", "PWD", "SHELL", "PYTHONPATH", "USER"]
        env = js["attributes"]["system"]["environment"]
        for envar in env:
            if envar in keep_names or envar.startswith("FLUX_"):
                keepers[envar] = env[envar]
        js["attributes"]["system"]["environment"] = keepers
    return js


class MultiprocessBulkRunner:
    """
    Uses a multiprocessing pool to perform parallel bulk submissions.

    Each worker process is assigned a chunk of commands and a single handle (URI).
    It writes its commands to a temporary file and executes `flux bulksubmit`
    via a subprocess, achieving high parallelism and throughput.
    """

    def __init__(self, handles):
        if not handles:
            raise ValueError("Runner requires at least one handle.")
        self.handles = handles
        self.jobs_to_complete = 0
        self.completion_event = None
        self.jobs = {}

    @staticmethod
    def _chunkify(data, num_chunks):
        if not data or num_chunks <= 0:
            return []
        chunks = [[] for _ in range(num_chunks)]
        for i, item in enumerate(data):
            chunks[i % num_chunks].append(item)
        return chunks

    @staticmethod
    def _async_cffi_worker(command_chunk, uri_string):
        """
        A pickle-safe worker that uses a pipelined CFFI approach for submission.
        """
        pid = os.getpid()
        handle = None
        handle = flux.Flux(uri_string)
        futures = []
        job_ids = []
        start_time = time.monotonic()

        # Submit all jobs without waiting for individual ACKs.
        # This blasts the broker with requests, just like bulksubmit.
        for command in command_chunk:
            spec = flux.job.JobspecV1.from_command(command)
            # submit_async is non-blocking, it returns a future immediately
            futures.append(flux.job.submit_async(handle, spec.dumps(), waitable=True))

        # Now, loop through the futures and get the IDs. The requests are
        # already "in-flight" or queued on the broker side.
        for f in futures:
            job_ids.append(f.get_id())

        end_time = time.monotonic()
        duration = end_time - start_time
        print(
            f"  - True Async CFFI Worker [{pid}] submitted {len(job_ids)} jobs in {duration:.2f}s."
        )
        return (len(command_chunk), start_time, end_time, job_ids, uri_string)

    @staticmethod
    def bulk_run_worker(command, uri_string, count):
        """
        Use original Flux Bulk Run for worker.
        """
        pid = os.getpid()
        handle = flux.Flux(uri_string)
        # This defaults to 1 task, 1 core == 1 slot
        spec = flux.job.JobspecV1.from_command(command)
        spec.setattr("system.exec.test.run_duration", "0.001s")

        start_time = time.monotonic()
        bulk = BulkRun(handle, count, spec).run()
        end_time = time.monotonic()
        duration = end_time - start_time
        print(f"  - Bulk Worker [{pid}] submitted {len(bulk.jobs)} jobs in {duration:.2f}s.")
        return (count, start_time, end_time, bulk.jobs, uri_string)

    @staticmethod
    def _bulk_submit_worker(command, uri_string, count):
        """
        Worker that uses `flux proxy` to correctly target a leaf broker and
        pipes commands to `flux bulksubmit` via stdin for maximum performance.
        """
        pid = os.getpid()

        input_string = "\n".join(shlex.join(command) for x in range(count))

        with tempfile.TemporaryFile(mode="w+") as stdout_pipe, tempfile.TemporaryFile(
            mode="w+"
        ) as stderr_pipe:
            cmd = ["flux", "proxy", uri_string, "flux", "bulksubmit", "--wait"]

            start_time = time.monotonic()
            subprocess.run(
                cmd,
                input=input_string,
                stdout=stdout_pipe,
                stderr=stderr_pipe,
                text=True,
                check=True,
            )
            end_time = time.monotonic()
            stdout_pipe.seek(0)
            duration = end_time - start_time
            handle = flux.Flux(uri_string)
            listing = flux.job.list.job_list(handle).get()["jobs"]

            # Add metadata organized as others (as we expect)
            jobs = {}
            for job in listing:
                print(job)
                job["clean"] = {"timestamp": job["t_cleanup"]}
                job["submit"] = {"timestamp": job["t_submit"]}
                jobs[job["id"]] = job
            print(
                f"  - Worker [{pid}] submitted {len(jobs)} jobs in {duration:.2f}s to {uri_string}."
            )
            return (count, start_time, end_time, jobs, uri_string)

    def distribute(self, total, slots):
        """
        Distribute a total count into some number of slots
        """
        # Break into base sizes
        base_count = total // slots
        # Distribute remainders
        remainder = total % slots
        slots = [base_count] * slots
        for i in range(remainder):
            slots[i] += 1
        return slots

    def run_clones(self, command, count):
        """
        Run many instances of an equivalent command.
        """
        num_handles = len(self.handles)
        print(
            f"\n=> Preparing parallel bulk submission for {count} jobs across {num_handles} handles..."
        )

        # Chunk command counts and pair with URIs for the workers.
        handle_uris = [h.uri for h in self.handles]
        counts = self.distribute(count, num_handles)
        print(counts)
        tasks = [(command, uri, count) for uri, count in zip(handle_uris, counts) if count]
        n_workers = min(os.cpu_count(), len(tasks))
        print(f"=> Creating a pool of {n_workers} worker processes...")
        results = []

        # 2. Run workers to perform parallel bulk submissions.
        # Note from V: I tested without starmap, same result.
        with multiprocessing.Pool(processes=n_workers) as pool:
            worker_args = [(command, chunk, uri) for command, chunk, uri in tasks]
            results += pool.starmap(self.bulk_run_worker, worker_args)
            print(f"=> All worker processes finished.")

        return summarize_results(results, count)

    def run(self, commands):
        """
        Distributes commands to a multiprocessing pool for parallel bulk submission.
        """
        if not commands:
            print("No commands to submit.")
            return {"total_submitted": 0}

        num_handles = len(self.handles)
        print(
            f"\n=> Preparing parallel bulk submission for {len(commands)} jobs across {num_handles} handles..."
        )

        # 1. Chunk commands and pair with URIs for the workers.
        command_chunks = self._chunkify(commands, num_handles)
        handle_uris = [h.uri for h in self.handles]
        tasks = [(c, u) for c, u in zip(command_chunks, handle_uris) if c]

        if not tasks:
            print("No tasks could be created for submission.")
            return {"total_submitted": 0}

        n_workers = min(os.cpu_count(), len(tasks))
        print(f"=> Creating a pool of {n_workers} worker processes...")

        # 2. Run workers to perform parallel bulk submissions.
        with multiprocessing.Pool(processes=n_workers) as pool:
            worker_args = [(chunk, uri) for chunk, uri in tasks]

            # starmap blocks until all workers are complete.
            # TODO this function needs to be written based off of bulk_run_worker
            # but instead allowing for unique commands.
            results = pool.starmap(self._bulk_submit_worker, worker_args)
            print(f"=> All worker processes finished.")

        # This likely needs refactor too for this case.
        return summarize_results(results, len(commands))

    def job_complete_cb(self, jobid, handle):
        """
        Callback executed when a job is complete. It fetches the job's
        final info and stores it in the shared dictionary.
        """
        try:
            self.jobs[jobid] = flux.job.get_job(handle, int(jobid))
        except Exception as e:
            print(f"There was an issue with job completion callback: {e}")
        finally:
            self.jobs_to_complete -= 1
            if self.jobs_to_complete <= 0:
                self.completion_event.set()

    def _job_complete_cb(self, future, jobid, handle):
        """
        Callback executed when a job is complete. It fetches the job's
        final info and stores it in the shared dictionary.
        """
        try:
            self.jobs[jobid] = flux.job.get_job(handle, int(jobid))
        except Exception as e:
            print(f"There was an issue with job completion callback: {e}")
        finally:
            self.jobs_to_complete -= 1
            if self.jobs_to_complete <= 0:
                self.completion_event.set()


class BulkRun:
    """
    This is the same BulkRun that flux src/tests/throughput.py uses.
    """

    def __init__(self, handle, total, jobspec):
        self.handle = handle
        self.total = total
        self.jobspec = jobspec
        self.jobs = {}
        self.submitted = 0
        self.running = 0
        self.complete = 0
        self.bbar = None

    def statusline(self, _bbar, _width):
        return (
            f"{self.total:>6} jobs: {self.submitted:>6} submitted, "
            f"{self.running:>6} running, {self.complete} completed"
        )

    def event_cb(self, future, jobid):
        event = future.get_event()
        if event is not None:
            if event.name == "submit":
                self.submitted += 1
                self.jobs[jobid][event.name] = event
            elif event.name == "start":
                self.running += 1
            elif event.name == "finish":
                self.running -= 1
                self.complete += 1
            elif event.name == "clean":
                self.jobs[jobid][event.name] = event
            if self.bbar:
                self.bbar.update()

    def handle_submit(self, jobid):
        self.jobs[jobid] = {"t_submit": time.time()}
        fut = flux.job.event_watch_async(self.handle, jobid)
        fut.then(self.event_cb, jobid)

    def submit_cb(self, future):
        # pylint: disable=broad-except
        try:
            self.handle_submit(future.get_id())
        except Exception as exc:
            print(f"Submission failed: {exc}", file=sys.stderr)

    def submit_async(self):
        spec = self.jobspec.dumps()
        for _ in range(self.total):
            flux.job.submit_async(self.handle, spec).then(self.submit_cb)

    def run(self):
        self.bbar = Bottombar(self.statusline).start()
        self.submit_async()
        self.handle.reactor_run()
        if self.bbar:
            self.bbar.stop()
        return self


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
