import glob
import json
import os


class Registry:
    """
    A file-based registry using atomic Bash commands.
    """

    def __init__(self, directory):
        self.directory = os.path.abspath(directory)
        os.makedirs(self.directory, exist_ok=True)

    def get_all(self):
        """
        Read all .json files in the registry directory.
        """
        results = []
        files = glob.glob(os.path.join(self.directory, "*.json"))

        for fpath in files:
            try:
                with open(fpath, "r") as fd:
                    data = json.load(fd)
                    results.append((data["id"], data["local_uri"], data["ssh_uri"]))
            except (json.JSONDecodeError, IOError):
                # Race condition: File exists but isn't fully flushed or readable yet.
                # Skip it and pick it up on the next polling loop.
                continue

        return results

    def get_register_block(self, uid, local_uri, ssh_uri_template):
        """
        Yield lines of Bash code to perform the atomic registration.

        Args:
            uid: Broker ID
            local_uri: Local URI string
            ssh_uri_template: String containing bash variables like $(hostname)
        """
        target = os.path.join(self.directory, f"{uid}.json")
        tmp_target = target + ".tmp"

        # Yield lines with newlines included for fd.write()
        yield f"cat > {tmp_target} <<EOF\n"
        yield "{\n"
        yield f'  "id": "{uid}",\n'
        yield f'  "local_uri": "{local_uri}",\n'
        yield f'  "ssh_uri": "{ssh_uri_template}"\n'
        yield "}\n"
        yield "EOF\n"
        yield f"mv {tmp_target} {target}\n"
