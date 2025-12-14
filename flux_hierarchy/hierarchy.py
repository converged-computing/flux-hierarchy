import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import flux_hierarchy.utils as utils
from flux_hierarchy.logger import LogColors
from flux_hierarchy.results import combine_results
from flux_hierarchy.runner import MultiprocessBulkRunner

here = os.path.abspath(os.path.dirname(__file__))

try:
    import flux
except ImportError:
    flux = None


class FluxBaseHierarchy:
    """
    A Flux Base Hierarchy includes shared functions between all different
    types of hierarchies.
    """

    def __init__(self):
        self.handles = {}
        self.uris = {}
        self.rank_lookup = None

    @property
    def resources(self):
        return self.config["resources"]

    @property
    def worker_exec(self):
        """
        Eventually we can have hierarchies creating hierarchies. Yeah, nuts.
        """
        return os.path.join(self.outdir, "local-worker.py")

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

    def connect(self):
        """
        Connect to all leaf broker URIs and store the handles.
        """
        if not self.uris:
            return

        # Time each one and show to user
        total = len(self.uris)
        i = 1
        self.pprint(f"Waiting for {total} leaf brokers...\n")
        for name, uri in self.uris.items():
            start = time.monotonic()
            self.handles[name] = flux.Flux(uri)
            self.handles[name].uri = uri
            end = time.monotonic()
            elapsed = end - start
            print(f"  => {i} broker took {elapsed} seconds.")
            i += 1
        self.pprint(f"Connected!\n")

    @property
    def rank_host_lookup(self):
        """
        Derive a lookup of ranks to hosts, and hosts to ranks.
        """
        if self.rank_lookup is not None:
            return self.rank_lookup
        lookup = {"hosts": {}, "ranks": {}}
        cmd = [
            "flux",
            "exec",
            "-r",
            "all",
            "/bin/bash",
            "-c",
            'echo "$(hostname):$(flux getattr rank)"',
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        for line in result.stdout.split("\n"):
            if not line.strip():
                continue
            host, rank = line.strip().split(":", 1)
            if host not in lookup["hosts"]:
                lookup["hosts"][host] = []
            lookup["hosts"][host].append(rank)
            lookup["ranks"][rank] = host
        self.rank_lookup = lookup
        return self.rank_lookup

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

    def submit_jobs(self, commands, equivalent=False):
        """
        Submit jobs using multiprocessing.

        This can be run from the root hierarchy worker, or from
        a node level worker. It can handle running clones or a single command.
        """
        self.pprint(f"Starting submission of {len(commands)} jobs...")

        # Instantiate and use the MultiprocessBulkRunner
        runner = MultiprocessBulkRunner(list(self.handles.values()))
        if equivalent:
            results = runner.run_clones(commands[0], total=len(commands))
        else:
            results = runner.run(commands, equivalent=True)

        self.pprint("Submission process complete.\n")
        return results


class FluxWorkerHierarchy(FluxBaseHierarchy):
    """
    A Flux Worker Hierarchy inherits commands from the Flux Hierarchy,
    but is expected to be run by a worker instance that has a pre-set
    number of uris, etc. that do not need to be created. This is expected
    to run on a single node and use all local URIs (that's the point).
    """

    def __init__(self, uris, clean_env=False):
        super().__init__()
        # These are complete, local uris that already exist
        self.uris = uris
        self.derive_handles(uris)
        self.clean_env = not clean_env

    def derive_handles(self, uris):
        """
        The uris come in as paths to socks, so derive handles from them!
        """
        self.check()
        self.handles = {}
        for uri in self.uris:
            flux_uri = f"local://{uri}"
            handle = flux.Flux(flux_uri)
            uid = os.path.basename(uri).replace(".sock", "")
            self.handles[uid] = handle
            self.handles[uid].uri = flux_uri

    def throughput(self, command, count=100):
        """
        Run local throughput (with local:// uris) on one node.
        """
        # Otherwise we stick with ssh:// directly from a main host
        print(f"Preparing throughput test for command: {' '.join(command)}")
        commands_list = [command for _ in range(count)]
        return self.submit_jobs(commands_list, equivalent=True)


class FluxHierarchy(FluxBaseHierarchy):
    """
    A FluxHierarchy allows for immediate or dynamic submission of
    jobs to a hierarchy of Flux instances. To start, we calculate
    instance sizes based on the resources given at the top level.
    """

    def __init__(self, config_path, outdir=None, prefix=None, clean_env=False):
        """
        Create an output directory for jobspecs and submit files.
        """
        super().__init__()

        # We will store a lookup of uris
        self.config = utils.read_yaml(config_path)
        self.prefix = prefix or ""
        self.jobid = None

        # You can't handle me right now.
        self.uris_by_host = {}
        self.init_structure(outdir)
        self.groups = {g["name"]: g for g in self.config["groups"]}
        self.clean_env = clean_env

        # Still thinking about this one. I think it should be possible
        # to define an entire tree, but then only create a subgraph of it
        # This is the entrypoint.
        self.entrypoint = self.config["entrypoint"]

    def init_structure(self, outdir):
        """
        Create output directories. This is only done by the top level,
        primary Flux Hierarchy (not a worker).
        """
        # Trying to make as short as possible
        self.outdir = outdir or tempfile.mkdtemp(prefix="fh-")
        self.socket_dir = os.path.join(self.outdir, "sock")
        self.logs_dir = os.path.join(self.outdir, "logs")

        # Hidden in local directory for results and uris
        self.local_dir = os.path.join(os.getcwd(), ".fh")
        self.uri_dir = os.path.join(self.local_dir, "uris")
        self.results_dir = os.path.join(self.local_dir, "results")
        if os.path.exists(self.local_dir):
            shutil.rmtree(self.local_dir)
        for path in self.socket_dir, self.logs_dir, self.uri_dir, self.results_dir:
            os.makedirs(path, exist_ok=True)

    def stage(self):
        """
        Stage the entire temporary directory across all nodes before start.

        This is done once by the main flux hierarchy to ensure all assets
        are ready / staged before we start anything.
        """
        # Write the worker submission script to the output directory
        shutil.copyfile(os.path.join(here, "worker.py"), self.worker_exec)
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

    def stop(self, cleanup=True):
        """
        Stop the hierarchy (cancel the job) if possible.

        This also cleans up.
        """
        if not self.jobid:
            return
        cmd = ["flux", "cancel", self.jobid]
        print(" ".join(cmd))
        utils.run_command(cmd, check_output=True)
        if not cleanup:
            return

    def cleanup(self):
        """
        Cleanup local cache and results directory in tmp.

        This doesn't cleanup results directories on worker nodes. We
        assume they will go away when allocation is cleared.
        """
        for path in self.local_dir, self.outdir:
            if os.path.exists(path):
                shutil.rmtree(path)

    def start(self, interactive=True):
        """
        Start a flux hierarchy of a specific size. This is only done
        by the main Flux Hierarchy and not the workers.
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

        # Save root jobid so we can cancel at the end.
        self.jobid = jobid

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

        # Generate the lookup of URI by hostname. We need to do this for
        # submission from the level of the node, using local URIs instead of ssh://
        self.organize_uris_by_host()

    def organize_uris_by_host(self):
        """
        Organize uris by hostname.
        """
        for _, uri in self.uris.items():
            # Parse URI: ssh://node01/tmp/sock -> host: node01, path: /tmp/sock
            if "ssh://" in uri:
                parts = uri.replace("ssh://", "").split(os.sep)
                host = parts[0]
                socket_path = os.sep + os.sep.join(parts[1:])
            else:
                # Handle local:// logic if running on 1 node
                host = socket.gethostname()
                socket_path = uri.replace("local://", "")

            if host not in self.uris_by_host:
                self.uris_by_host[host] = []
            self.uris_by_host[host].append(socket_path)

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
        print("    " + " ".join(command))
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

    def throughput(self, command, count=100, local=False):
        """
        A specialized function to test throughput by submitting one command many times.
        """
        # Local throughput means we submit to individual nodes and use a local:// uri
        if local:
            return self.local_throughput(command, count)

        # Otherwise we stick with ssh:// directly from a main host
        print(f"Preparing throughput test for command: {' '.join(command)}")
        commands_list = [command for _ in range(count)]
        return self.submit_jobs(commands_list, equivalent=True)

    def local_throughput(self, command, count=100):
        """
        Modified throughput to submit to single nodes.
        """
        print(f"Preparing throughput test for command: {' '.join(command)}")
        total_brokers = len(self.uris)
        total_nodes = len(self.uris_by_host)
        jobs_per_broker = count // total_brokers
        jobs_per_node = count // total_nodes

        # Submit Worker Jobs on the level of the node.
        # Each worker job will run flux-hierarchy
        print(
            f"Distributing {count} jobs to {len(self.uris_by_host)} nodes ({total_brokers} total brokers, {jobs_per_broker} jobs per broker)..."
        )

        # We need a mapping of hostnames to ranks.
        # I think there is a way to do this with Flux - I can't reproduce it now.
        lookup = self.rank_host_lookup

        # Grab the shortest path for the host, which should be highest in the tree
        for host, sockets in self.uris_by_host.items():
            result_file = os.path.join(self.results_dir, f"{host}.json")

            # IMPORTANT: this count is the TOTAL across sockets here.
            # Meaning the total for the node (but not uris)
            payload = {
                "sockets": sockets,
                "commands": [command],
                "count": jobs_per_node,
                "clones": True,
                "result_file": result_file,
                "clean_env": self.clean_env,
            }

            # This is the host to submit to
            submit_to_rank = lookup["hosts"][host][0]

            # Use flux python to avoid needing to know pythonpath, etc.
            cmd = [
                "flux",
                "exec",
                "-r",
                submit_to_rank,
                "--bg",
                f"--jobid={self.jobid}",
                sys.executable,
                self.worker_exec,
            ]
            print(" ".join(cmd))
            cmd.append(json.dumps(payload))

            # Use flux exec in the background to the jobid
            # If we do a flux run/submit here, we need resources that may not exist
            subprocess.run(cmd, capture_output=True, text=True, check=True)

        # Wait for results. Destroy the filesystem! Just kidding.
        # This is imperfect - instead of relying on Flux wait (maybe better?) we wait for files
        print("Waiting for workers...")
        while len(os.listdir(self.results_dir)) < len(self.uris_by_host):
            time.sleep(1)

        # Wait for write finished
        is_writing = True
        while is_writing:
            files = os.listdir(self.results_dir)
            is_writing = any(x for x in files if x.endswith(".lock"))

        return combine_results(self.results_dir)


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
