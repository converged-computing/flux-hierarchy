import multiprocessing
import socket
import sys
import time

import requests

from ..base import RegistryInterface
from .api import run_api


class ServerRegistry(RegistryInterface):
    """
    A Web-based registry using FastAPI.
    Spawns the server in a background process.
    """

    def __init__(self, host=None, port=8000):
        # Default to actual IP so remote nodes can reach us (not localhost)
        self.host = host or socket.gethostbyname(socket.gethostname())
        self.port = port
        self.server_process = None
        self.api_url = f"http://{self.host}:{self.port}"

    def start(self):
        """
        Start the uvicorn server in a separate process.
        """
        if self.server_process and self.server_process.is_alive():
            return

        print(f"  -> Starting Registry Server at {self.api_url}...")
        self.server_process = multiprocessing.Process(target=run_api, args=(self.host, self.port))
        self.server_process.daemon = True  # Kill if main process dies
        self.server_process.start()

        # Wait for it to come up
        for _ in range(20):
            try:
                requests.get(f"{self.api_url}/brokers", timeout=1)
                return
            except requests.exceptions.ConnectionError:
                time.sleep(0.2)
        print("  -> Warning: Registry server might not be ready.")

    def stop(self):
        """
        Kill the server.
        """
        if self.server_process:
            self.server_process.terminate()
            self.server_process.join()

    def get_uris(self):
        """
        Query the API for all brokers.
        """
        try:
            resp = requests.get(f"{self.api_url}/brokers")
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return []

    def register(self, uid, local_uri, socket_path, hostname):
        """
        Manual registration method (for the root job running locally).
        """
        payload = {
            "id": uid,
            "local_uri": local_uri,
            "socket_path": socket_path,
            "hostname": hostname,
        }
        try:
            requests.post(f"{self.api_url}/register", json=payload)
        except Exception as e:
            print(f"Failed to register local root: {e}")

    def get_registration_code(self, uid, local_uri):
        """
        Return a Bash command to register a broker using curl.
        """
        sock_path = local_uri.replace("local://", "")
        
        # We construct a JSON string for Bash.
        # We use double quotes for the -d argument so $(hostname) expands.
        # We must escape the internal double quotes for JSON.
        json_payload = (
            f'{{\\"id\\": \\"{uid}\\", '
            f'\\"local_uri\\": \\"{local_uri}\\", '
            f'\\"socket_path\\": \\"{sock_path}\\", '
            f'\\"hostname\\": \\"$(hostname)\\"}}'
        )

        # curl flags:
        # -s: Silent (no progress bar)
        # -S: Show errors
        # --retry 5: Retry 5 times
        # --retry-connrefused: Retry even if connection is refused (server starting up)
        # --retry-delay 1: Wait 1s between retries
        cmd = (
            f"curl -s -S --retry 10 --retry-delay 1 --retry-connrefused "
            f"-X POST -H 'Content-Type: application/json' "
            f"-d \"{json_payload}\" "
            f"{self.api_url}/register"
        )
        
        return cmd