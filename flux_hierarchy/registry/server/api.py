from typing import Dict, List

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel


# Data Model for a Broker Registration
class BrokerInfo(BaseModel):
    id: str
    local_uri: str
    socket_path: str
    hostname: str


# In-memory storage for the lifecycle of this process
brokers: Dict[str, BrokerInfo] = {}

app = FastAPI(title="Flux Hierarchy Registry")


@app.post("/register")
async def register(info: BrokerInfo):
    """
    Register a new broker.
    """
    brokers[info.id] = info
    # Construct SSH URI immediately so it's ready for consumers
    ssh_uri = f"ssh://{info.hostname}{info.socket_path}"
    return {"status": "registered", "ssh_uri": ssh_uri}


@app.get("/brokers")
async def get_brokers():
    """
    Get all registered brokers in the tuple format required by hierarchy.
    """
    results = []
    for b in brokers.values():
        ssh_uri = f"ssh://{b.hostname}{b.socket_path}"
        results.append((b.id, b.local_uri, ssh_uri))
    return results


def run_api(host: str, port: int):
    """
    Entrypoint for the multiprocessing Process.
    """
    uvicorn.run(app, host=host, port=port, log_level="error")
