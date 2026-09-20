from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from typing import Any, Callable, Dict, List, Optional

import networkx as nx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from src.antigravity.core.engine import AsyncTaskEngine
from src.antigravity.pipeline.graph import DAGExecutor
from src.antigravity.storage.adapter import TaskStorageAdapter


FUNCTION_REGISTRY: Dict[str, Callable[..., Any]] = {
    "identity": lambda value: value,
    "add": lambda a, b: a + b,
    "multiply": lambda a, b: a * b,
    "sum": lambda *values: sum(values),
    "double": lambda value: value * 2,
    "echo": lambda value: value,
}


class NodeRequest(BaseModel):
    id: str
    function: str = "identity"
    depends_on: List[str] = Field(default_factory=list)
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    retries: int = 3
    timeout: Optional[float] = None


class TaskSubmitRequest(BaseModel):
    nodes: List[NodeRequest]


engine = AsyncTaskEngine(max_workers=4)
storage = TaskStorageAdapter(db_path="antigravity.db")
dag_executor = DAGExecutor(engine=engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await storage.initialize()
    yield
    await engine.close()
    await storage.close()


app = FastAPI(title="Antigravity", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "runtime": engine.metrics_snapshot(),
        "storage": {"database": storage.db_path},
    }


@app.post("/tasks/submit")
async def submit_task_graph(payload: TaskSubmitRequest) -> Dict[str, Any]:
    if not payload.nodes:
        raise HTTPException(status_code=400, detail="At least one node is required.")

    graph = nx.DiGraph()
    for node in payload.nodes:
        if node.function not in FUNCTION_REGISTRY:
            raise HTTPException(status_code=400, detail=f"Unsupported function: {node.function}")
        graph.add_node(
            node.id,
            func=FUNCTION_REGISTRY[node.function],
            args=tuple(node.args),
            kwargs=node.kwargs,
            retries=node.retries,
            timeout=node.timeout,
        )
        for dependency in node.depends_on:
            graph.add_edge(dependency, node.id)

    task_id = f"task-{uuid.uuid4().hex[:12]}"
    await storage.upsert_task(task_id, "queued", payload=payload.model_dump())

    try:
        results = await dag_executor.execute(graph, input_payload={"task_id": task_id})
        await storage.upsert_task(
            task_id,
            "succeeded",
            payload=payload.model_dump(),
            result={"results": results},
            metrics=engine.metrics_snapshot(),
        )
        return {"task_id": task_id, "status": "succeeded", "results": results}
    except Exception as exc:  # pragma: no cover - exercised via endpoint tests
        await storage.upsert_task(task_id, "failed", payload=payload.model_dump(), error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/tasks/{task_id}")
async def get_task(task_id: str) -> Dict[str, Any]:
    task = await storage.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return task
