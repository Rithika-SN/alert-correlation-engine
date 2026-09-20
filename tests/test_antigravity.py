import networkx as nx
import pytest
from fastapi.testclient import TestClient

from src.antigravity.core.engine import AsyncTaskEngine, TaskSpec, TaskState
from src.antigravity.pipeline.graph import DAGExecutor
from src.antigravity.api.main import app


@pytest.mark.asyncio
async def test_task_queueing_and_retry():
    engine = AsyncTaskEngine(max_workers=2)
    attempts = {"count": 0}

    async def flaky_task(value: int) -> int:
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise ValueError("transient failure")
        return value * 2

    task = TaskSpec(func=flaky_task, args=(3,), max_retries=2, retry_backoff=0.01)
    task_id = await engine.submit(task)
    result = await engine.wait_for_task(task_id)

    assert result.state is TaskState.SUCCEEDED
    assert result.result == 6
    assert attempts["count"] == 2
    await engine.close()


@pytest.mark.asyncio
async def test_dag_execution_order():
    engine = AsyncTaskEngine(max_workers=2)
    executor = DAGExecutor(engine=engine)
    graph = nx.DiGraph()
    graph.add_node("source", func=lambda: 3)
    graph.add_node("left", func=lambda value: value + 2)
    graph.add_node("right", func=lambda value: value * 4)
    graph.add_node("final", func=lambda left, right: left + right)
    graph.add_edge("source", "left")
    graph.add_edge("source", "right")
    graph.add_edge("left", "final")
    graph.add_edge("right", "final")

    result = await executor.execute(graph)

    assert result["left"] == 5
    assert result["right"] == 12
    assert result["final"] == 17
    await engine.close()


@pytest.mark.asyncio
async def test_failure_recovery():
    engine = AsyncTaskEngine(max_workers=2)
    attempts = {"count": 0}

    def flaky_value() -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("still failing")
        return "recovered"

    task = TaskSpec(func=flaky_value, max_retries=3, retry_backoff=0.01)
    task_id = await engine.submit(task)
    result = await engine.wait_for_task(task_id)

    assert result.state is TaskState.SUCCEEDED
    assert result.result == "recovered"
    assert attempts["count"] == 3
    await engine.close()


def test_api_health_and_submit_endpoints():
    with TestClient(app) as client:
        health_response = client.get("/health")
        assert health_response.status_code == 200
        assert health_response.json()["status"] == "ok"

        payload = {
            "nodes": [
                {"id": "base", "function": "double", "args": [2]},
                {"id": "final", "function": "sum", "depends_on": ["base"], "args": [5]},
            ]
        }
        submit_response = client.post("/tasks/submit", json=payload)
        assert submit_response.status_code == 200
        body = submit_response.json()
        assert body["status"] == "succeeded"
        assert body["results"]["final"] == 9
