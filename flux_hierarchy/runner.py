import multiprocessing
import os
import sys
import time

from flux_hierarchy.results import summarize_results

try:
    import flux
    import flux.job
    from flux.progress import Bottombar
except ImportError:
    flux = None


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

    def _run_tasks(self, func, args, count):
        """
        Shared run function to run a set of tasks and return results.
        """
        print(
            f"\n=> Preparing parallel bulk submission for {count} jobs across {len(self.handles)} handles..."
        )

        n_workers = min(os.cpu_count(), len(self.handles))
        print(f"=> Creating a pool of {n_workers} worker processes...")
        results = []

        # 2. Run workers to perform parallel bulk submissions.
        # Note from V: I tested without starmap, same result.
        with multiprocessing.Pool(processes=n_workers) as pool:
            results += pool.starmap(func, args)
            print(f"=> All worker processes finished.")

        # We provide the global (total) count to assert we have all the
        # start and completion (clean) times.
        return summarize_results(results, count)

    @property
    def handle_uris(self):
        """
        The worker just has the listing of uris.
        They aren't actually handles.
        """
        uris = []
        for handle in self.handles:
            if hasattr(handle, "uri"):
                uris.append(handle.uri)
            else:
                uris.append(handle)
        return uris

    def run_clones(self, command, total):
        """
        Run many instances of an equivalent command.
        """
        num_handles = len(self.handles)
        counts = self.distribute(total, num_handles)
        print(self.handles)
        print(counts)
        args = [(command, uri, count) for uri, count in zip(self.handle_uris, counts) if count]
        return self._run_tasks(bulk_run_clones, args, total)

    def run(self, commands):
        """
        Distributes commands to a multiprocessing pool for parallel bulk submission.
        """
        num_handles = len(self.handles)
        command_chunks = chunkify(commands, num_handles)
        args = [(c, u) for c, u in zip(command_chunks, self.handle_uris) if c]
        return self._run_tasks(bulk_run, args, len(commands))

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


class BulkRunBase:
    """
    Base class for BulkRunner. This is what flux src/tests/throughput.py
    uses. We will customize the wrapping (command, parsing, etc.)
    """

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
        try:
            self.handle_submit(future.get_id())
        except Exception as exc:
            print(f"Submission failed: {exc}", file=sys.stderr)

    def run(self):
        self.bbar = Bottombar(self.statusline).start()
        self.submit_async()
        self.handle.reactor_run()
        if self.bbar:
            self.bbar.stop()
        return self


class BulkRun(BulkRunBase):
    """
    This is the same BulkRun that flux src/tests/throughput.py uses.

    # TODO: expose the run_duration if needed/desired. Right now we simulate.
    """

    def __init__(self, handle, commands):
        self.handle = handle
        self.commands = commands
        self.jobs = {}
        self.submitted = 0
        self.running = 0
        self.complete = 0
        self.bbar = None

    @property
    def total(self):
        return len(self.commands)

    def submit_async(self):
        # Here is the main difference - assembling the jobspec (unique) for each
        # We haven't exposed changing resources, etc. here (and probably should)
        for command in self.commands:
            spec = flux.job.JobspecV1.from_command(command)
            spec.setattr("system.exec.test.run_duration", "0.001s")
            spec = spec.dumps()
            flux.job.submit_async(self.handle, spec).then(self.submit_cb)


class BulkRunClones(BulkRunBase):
    """
    This is the same BulkRun that flux src/tests/throughput.py uses.
    """

    def __init__(self, handle, command, total):
        self.handle = handle
        self.total = total
        self.command = command
        self.jobs = {}
        self.submitted = 0
        self.running = 0
        self.complete = 0
        self.bbar = None

    def submit_async(self):
        # This defaults to 1 task, 1 core == 1 slot
        spec = flux.job.JobspecV1.from_command(self.command)
        spec.setattr("system.exec.test.run_duration", "0.001s")
        spec = spec.dumps()
        for _ in range(self.total):
            flux.job.submit_async(self.handle, spec).then(self.submit_cb)


def chunkify(data, num_chunks):
    """
    Turn some list of data into a number of chunks.
    """
    if not data or num_chunks <= 0:
        return []
    chunks = [[] for _ in range(num_chunks)]
    for i, item in enumerate(data):
        chunks[i % num_chunks].append(item)
    return chunks


def bulk_run_clones(command, uri_string, count):
    """
    Use original Flux Bulk Run for worker.
    """
    pid = os.getpid()
    handle = flux.Flux(uri_string)
    start_time = time.monotonic()
    bulk = BulkRunClones(handle, command, count).run()
    end_time = time.monotonic()
    duration = end_time - start_time
    print(f"  - Bulk Worker [{pid}] submitted {len(bulk.jobs)} jobs in {duration:.2f}s.")
    return (count, start_time, end_time, bulk.jobs, uri_string)


def bulk_run(commands, uri_string):
    """
    Use original Flux Bulk Run for worker.
    """
    pid = os.getpid()
    handle = flux.Flux(uri_string)
    start_time = time.monotonic()
    bulk = BulkRun(handle, commands).run()
    end_time = time.monotonic()
    duration = end_time - start_time
    print(f"  - Bulk Worker [{pid}] submitted {len(bulk.jobs)} jobs in {duration:.2f}s.")
    return (len(commands), start_time, end_time, bulk.jobs, uri_string)
