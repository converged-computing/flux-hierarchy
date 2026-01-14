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
from flux_hierarchy.registry import get_registry
from flux_hierarchy.results import combine_results
from flux_hierarchy.runner import MultiprocessBulkRunner

here = os.path.abspath(os.path.dirname(__file__))

try:
    import flux
except ImportError:
    flux = None


class FluxBaseHierarchy:
    def __init__(self):
        self.handles = {}
        self.uris = {}
        self.rank_lookup = None

    @property
    def resources(self):
        return self.config["resources"]

    @property
    def worker_exec(self):
        return os.path.join(self.outdir, "local-worker.py")

    def pprint(self, message):
        print(f"=> {LogColors.OKCYAN}{message}{LogColors.ENDC}", end="")

    def check(self):
        if not flux:
            raise ValueError("Cannot import flux, which is needed here.")
        self.local_uri = os.environ.get("FLUX_URI")

    def view(self):
        self.pprint(f"\n🌿 Leaf Broker Workers...")
        print(json.dumps(self.uris, indent=2))
        self.print_tree()

    def connect(self):
        if not self.uris:
            return
        total = len(self.uris)
        i = 1
        self.pprint(f"Connecting to {total} leaf brokers for inspection...\n")
        for name, uri in self.uris.items():
            try:
                self.handles[name] = flux.Flux(uri)
                self.handles[name].uri = uri
            except Exception as e:
                print(f"  [WARN] Could not connect to {name}: {e}")
            i += 1
        self.pprint(f"Connected!\n")

    @property
    def rank_host_lookup(self):
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
        self._print_tree_recursive(self.entrypoint)

    def _print_tree_recursive(self, group_name, prefix="", is_last=True):
        group = self.groups[group_name]
        resource_key = group["resources"]
        resource_def = self.resources[resource_key]

        parts = []
        if "nodes" in resource_def:
            parts.append(f"Nodes: {resource_def['nodes']}")
        if "cores" in resource_def:
            parts.append(f"Cores: {resource_def['cores']}")
        if "count" in resource_def:
            parts.append(f"Tasks: {resource_def['count']}")
        result = f"[{', '.join(parts)}]"

        is_leaf = "launch" not in group
        branch_char = "└── " if is_last else "├── "
        name_color = LogColors.OKGREEN if is_leaf else LogColors.OKBLUE

        if prefix == "":
            print(f"{name_color}{LogColors.BOLD}{group_name}{LogColors.ENDC} {result}")
        else:
            print(f"{prefix}{branch_char}{name_color}{group_name}{LogColors.ENDC} {result}")

        if not is_leaf:
            children_to_launch = []
            for task in group.get("launch", []):
                count = task.get("count", 1)
                children_to_launch.extend([task["group"]] * count)

            num_children = len(children_to_launch)
            for i, child_name in enumerate(children_to_launch):
                is_child_last = i == num_children - 1
                new_prefix = prefix + ("    " if is_last else "│   ")
                self._print_tree_recursive(child_name, new_prefix, is_child_last)

    def submit_jobs(self, commands, equivalent=False):
        """
        Submit jobs using distributed workers.
        """
        valid_hosts = {h: s for h, s in self.uris_by_host.items() if len(s) > 0}
        total_nodes = len(valid_hosts)
        
        if total_nodes == 0:
            print("Error: No leaf brokers found to submit to!")
            return []

        count = len(commands)
        self.pprint(f"Distributing {count} jobs to {total_nodes} nodes...\n")

        jobs_per_node = count // total_nodes
        lookup = self.rank_host_lookup

        for host, sockets in valid_hosts.items():
            result_file = os.path.join(self.results_dir, f"{host}.json")
            payload_commands = [commands[0]] if equivalent else commands

            payload = {
                "sockets": sockets,
                "commands": payload_commands,
                "count": jobs_per_node,
                "clones": equivalent,
                "result_file": result_file,
                "clean_env": self.clean_env,
            }

            if host in lookup["hosts"]:
                submit_to_rank = lookup["hosts"][host][0]
                
                cmd = [
                    "flux", "exec", "-r", submit_to_rank,
                    "--bg", f"--jobid={self.jobid}",
                    sys.executable, self.worker_exec,
                ]
                print(" ".join(cmd))
                cmd.append(json.dumps(payload))
                subprocess.run(cmd, capture_output=True, text=True, check=True)
            else:
                print(f"Warning: Host {host} has brokers but no top-level rank to submit from.")

        print("Waiting for workers...")
        while len(os.listdir(self.results_dir)) < total_nodes:
            time.sleep(1)

        is_writing = True
        while is_writing:
            files = os.listdir(self.results_dir)
            is_writing = any(x for x in files if x.endswith(".lock"))
            time.sleep(0.1)

        self.pprint("Distributed submission complete.\n")
        return combine_results(self.results_dir)


class FluxWorkerHierarchy(FluxBaseHierarchy):
    def __init__(self, uris, clean_env=False):
        super().__init__()
        self.uris = uris
        self.derive_handles(uris)
        self.clean_env = not clean_env

    def derive_handles(self, uris):
        self.check()
        self.handles = {}
        for uri in self.uris:
            flux_uri = f"local://{uri}"
            handle = flux.Flux(flux_uri)
            uid = os.path.basename(uri).replace(".sock", "")
            self.handles[uid] = handle
            self.handles[uid].uri = flux_uri

    def throughput(self, command, count=100):
        print(f"Preparing throughput test for command: {' '.join(command)}")
        commands_list = [command for _ in range(count)]
        # FluxWorkerHierarchy inherits from FluxBaseHierarchy
        # We need to use the local multiprocessing logic here, not the distributed one.
        # Since FluxBaseHierarchy.submit_jobs is distributed, we use the MultiprocessBulkRunner directly here
        # or implement a local_submit_jobs in this class.
        
        runner = MultiprocessBulkRunner(list(self.handles.values()))
        return runner.run_clones(commands_list[0], total=count)
    
    def submit_jobs(self, commands):
        runner = MultiprocessBulkRunner(list(self.handles.values()))
        return runner.run(commands, equivalent=True)


class FluxHierarchy(FluxBaseHierarchy):
    def __init__(self, config_path, outdir=None, prefix=None, clean_env=False):
        super().__init__()
        self.config = utils.read_yaml(config_path)
        self.prefix = prefix or ""
        self.jobid = None
        self.uris_by_host = {}

        self.registry = get_registry("server", port=8000)
        self.groups = {g["name"]: g for g in self.config["groups"]}
        self.clean_env = clean_env
        self.entrypoint = self.config["entrypoint"]
        self.hostname = utils.run_command(["hostname"])["message"].strip()
        
        self.init_structure(outdir)

    def init_structure(self, outdir):
        """
        Init directory structure for work.
        """
        self.outdir = outdir or tempfile.mkdtemp(prefix="fh-")
        self.socket_dir = os.path.join(self.outdir, "sock")
        self.local_dir = os.path.join(os.getcwd(), ".fh")
        self.logs_dir = os.path.join(self.local_dir, "logs")
        self.results_dir = os.path.join(self.local_dir, "results")
        
        if os.path.exists(self.local_dir):
            shutil.rmtree(self.local_dir)
            
        for path in self.socket_dir, self.logs_dir, self.results_dir:
            os.makedirs(path, exist_ok=True)

    def stage(self):
        shutil.copyfile(os.path.join(here, "worker.py"), self.worker_exec)
        name = os.path.basename(self.outdir)
        
        cmd = ["flux", "archive", "create", "-C", self.outdir, "--name", name, "."]
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # We also do this in the job wrapper to be 100% sure.
        cmd = ["flux", "exec", "-r", "all", "mkdir", "-p", self.socket_dir]
        subprocess.run(cmd, capture_output=True, text=True, check=False)
        
        cmd = [
            "flux",
            "exec",
            "-r",
            "all",
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
        if not self.jobid:
            return
        cmd = ["flux", "cancel", self.jobid]
        print(" ".join(cmd))
        utils.run_command(cmd, check_output=True)
        self.registry.stop()
        if not cleanup:
            return

    def cleanup(self):
        for path in self.local_dir, self.outdir:
            if os.path.exists(path):
                shutil.rmtree(path)

    def start(self, interactive=True):
        self.check()
        self.registry.start()

        try:
            self.pprint(f"🌲 Generating Flux Hierarchy...\n")
            filepath, is_leaf_group = self.generate(self.entrypoint, "0")

            self.stage()

            self.pprint(f"\n🚗 Starting...\n")
            cmd = ["flux", "job", "submit", "--flags=waitable", filepath]
            print(" ".join(cmd))
            jobid = utils.run_command(cmd, check_output=True)["message"].strip()
            
            self.jobid = jobid

            if is_leaf_group:
                self.save_uri(jobid, "0")

            self.pprint(f"\n🌿 Leaf Broker Workers...")
            self.load_uris()
            print(json.dumps(self.uris, indent=2))
            
            self.print_tree()

            self.connect()
            if interactive:
                self.interactive()

            return self.uris
            
        finally:
            self.registry.stop()

    def save_uri(self, jobid, uri_id):
        local_uri = utils.run_command(["flux", "uri", "--wait", jobid], check_output=True)[
            "message"
        ].strip()

        nodes_raw = utils.run_command(
            ["flux", "jobs", "-n", "-o", "{nodelist}", jobid], check_output=True
        )["message"].strip()
        
        host = utils.run_command(
            ["flux", "hostlist", "--expand", nodes_raw], check_output=True
        )["message"].strip().split("\n")[0]

        sock_path = local_uri.replace("local://", "")
        self.registry.register(uri_id, local_uri, sock_path, host)

    def calculate_leaf_brokers(self, group_name):
        group = self.groups[group_name]
        if "launch" not in group:
            return 1
        total = 0
        for task in group["launch"]:
            count = task.get("count", 1)
            total += count * self.calculate_leaf_brokers(task["group"])
        return total

    def load_uris(self):
        expected_count = self.calculate_leaf_brokers(self.entrypoint)
        self.pprint(f"Polling Web Registry for {expected_count} leaf brokers...\n")

        # Basic wait loop
        while True:
            rows = self.registry.get_uris()
            current_count = len(rows)
            print(f"  ... found {current_count}/{expected_count} brokers", end='\r')
            
            if current_count >= expected_count:
                break
            time.sleep(1.0)

        self.uris = {}
        for row in rows:
            uid, local, remote = row
            if f"/{self.hostname}/" in remote or self.hostname in remote:
                self.uris[uid] = local
            else:
                self.uris[uid] = remote

        print(f"\nAll {len(self.uris)} brokers registered.\n")
        self.organize_uris_by_host()

    def organize_uris_by_host(self):
        for _, uri in self.uris.items():
            if "ssh://" in uri:
                parts = uri.replace("ssh://", "").split(os.sep)
                host = parts[0]
                socket_path = os.sep + os.sep.join(parts[1:])
            else:
                host = socket.gethostname()
                socket_path = uri.replace("local://", "")

            if host not in self.uris_by_host:
                self.uris_by_host[host] = []
            self.uris_by_host[host].append(socket_path)

    def interactive(self):
        print(f"\n{LogColors.BOLD}Dropping into an interactive IPython shell.{LogColors.ENDC}")
        import IPython
        IPython.embed()

    def generate(self, group_name, instance_path):
        """
        Recursively generate jobspecs and scripts.
        """
        print(f"- Generating: {group_name} (instance: {instance_path})")
        group = self.groups[group_name]
        is_leaf_group = "launch" not in group
        label = group["resources"]
        
        log_path = os.path.join(self.logs_dir, group_name + ".out")
        jobspec_filename = os.path.abspath(
            os.path.join(self.outdir, f"jobspec-{group_name}-{instance_path}.json")
        )
        
        # We still need to ensure the parent dir (socket_dir) exists on the remote node
        socket_path = os.path.abspath(os.path.join(self.socket_dir, f"{instance_path}.sock"))
        uri_string = f"local://{socket_path}"        
        socket_dir = os.path.dirname(socket_path)
        debug_wrapper = (
            f"mkdir -p {socket_dir} && "
            f"exec flux broker -S local-uri={uri_string}"
        )

        if is_leaf_group:
            broker_start_script = self.write_broker_start(instance_path, uri_string)
            # Wrap in bash to execute the wrapper logic
            real_cmd = f"{debug_wrapper} /bin/bash {broker_start_script}"
            command = ["/bin/bash", "-c", real_cmd]
        else:
            child_paths = {}
            for task in group["launch"]:
                name = task["group"]
                count = task.get("count") or 1
                for i in range(count):
                    task_path = f"{instance_path}-{i}"
                    child_path, _ = self.generate(name, task_path)
                    child_paths[task_path] = child_path

            script_path = os.path.abspath(
                os.path.join(self.outdir, f"inner-script-{group_name}-{instance_path}.sh")
            )
            
            script = self.generate_script(child_paths, instance_path, uri_string)
            utils.write_file(script, script_path, executable=True)

            real_cmd = f"{debug_wrapper} {script_path}"
            command = ["/bin/bash", "-c", real_cmd]

        jobspec = get_jobspec_from_dry_run(
            command, self.resources[label], log_path, clean_env=self.clean_env
        )
        utils.write_json(jobspec, jobspec_filename)
        return jobspec_filename, is_leaf_group

    def write_broker_start(self, instance_path, uri_string):
        """
        Write a Bash script to start the broker and register via curl.
        """
        filename = f"broker-start-{instance_path}.sh"
        script_path = os.path.join(self.outdir, filename)

        with open(script_path, "w") as fd:
            fd.write("#!/bin/bash\n")
            fd.write(self.registry.get_registration_code(instance_path, uri_string))
            fd.write("\n")
            fd.write("sleep infinity\n")
        
        os.chmod(script_path, 0o755)
        return script_path

    def generate_script(self, child_paths, instance_path, uri_string):
        script = "#!/bin/bash\n"
        script += "set -euo pipefail\n\n"
        for _, child_path in child_paths.items():
            script += f"flux job submit --flags=waitable {child_path}\n"
        script += "\nflux job wait --all\n"
        return script

    def throughput(self, command, count=100):
        print(f"Preparing throughput test for command: {' '.join(command)}")
        commands_list = [command for _ in range(count)]
        return self.submit_jobs(commands_list, equivalent=True)

    def submit_jobs(self, commands, equivalent=False):
        """
        Submit jobs using distributed workers via flux exec --bg.
        """
        valid_hosts = {h: s for h, s in self.uris_by_host.items() if len(s) > 0}
        total_nodes = len(valid_hosts)
        
        if total_nodes == 0:
            print("Error: No leaf brokers found to submit to!")
            return []

        count = len(commands)
        self.pprint(f"Distributing {count} jobs to {total_nodes} nodes...\n")

        jobs_per_node = count // total_nodes
        lookup = self.rank_host_lookup

        for host, sockets in valid_hosts.items():
            result_file = os.path.join(self.results_dir, f"{host}.json")
            payload_commands = [commands[0]] if equivalent else commands

            payload = {
                "sockets": sockets,
                "commands": payload_commands,
                "count": jobs_per_node,
                "clones": equivalent,
                "result_file": result_file,
                "clean_env": self.clean_env,
            }

            if host in lookup["hosts"]:
                submit_to_rank = lookup["hosts"][host][0]
                
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
                
                subprocess.run(cmd, capture_output=True, text=True, check=True)
            else:
                print(f"Warning: Host {host} has brokers but no top-level rank to submit from.")

        print("Waiting for workers...")
        while len(os.listdir(self.results_dir)) < total_nodes:
            time.sleep(1)

        is_writing = True
        while is_writing:
            files = os.listdir(self.results_dir)
            is_writing = any(x for x in files if x.endswith(".lock"))
            time.sleep(0.1)

        self.pprint("Distributed submission complete.\n")
        return combine_results(self.results_dir)


def get_jobspec_from_dry_run(command, resources, log_path=None, clean_env=True):
    # (Same as before)
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

    if clean_env:
        keepers = {}
        keep_names = ["PATH", "PWD", "SHELL", "PYTHONPATH", "USER"]
        env = js["attributes"]["system"]["environment"]
        for envar in env:
            if envar in keep_names or envar.startswith("FLUX_"):
                keepers[envar] = env[envar]
        js["attributes"]["system"]["environment"] = keepers
    return js