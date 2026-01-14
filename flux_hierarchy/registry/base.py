from abc import ABC, abstractmethod


class RegistryInterface(ABC):
    """
    Abstract Base Class for Flux Hierarchy Registries.
    """

    @abstractmethod
    def start(self):
        """Start any background processes (if needed)."""
        pass

    @abstractmethod
    def stop(self):
        """Stop/Cleanup background processes."""
        pass

    @abstractmethod
    def get_uris(self):
        """
        Return list of tuples: [(id, local_uri, ssh_uri), ...]
        """
        pass

    @abstractmethod
    def register(self, uid, local_uri, socket_path, hostname):
        """
        Manual registration from the head node/python script.
        """
        pass

    @abstractmethod
    def get_registration_code(self, uid, local_uri):
        """
        Yield lines of code (Bash or Python-snippet) that a worker node
        can execute to register itself.
        """
        pass
