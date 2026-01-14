from .server import ServerRegistry


def get_registry(backend="server", **kwargs):
    """
    Factory to return a registry backend.

    Args:
        backend (str): 'server' (default). Future: 'file', 'redis', etc.
        **kwargs: Arguments passed to the backend constructor (e.g. port).
    """
    if backend == "server":
        return ServerRegistry(**kwargs)

    raise ValueError(f"Unknown registry backend: {backend}")
