from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

import networkx as nx

from src.antigravity.core.engine import AsyncTaskEngine, TaskSpec, TaskState


@dataclass
class DAGNode:
    node_id: str
    func: Callable[..., Any]
    depends_on: tuple[str, ...] = ()
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    retries: int = 3
    retry_backoff: float = 0.25
    timeout: Optional[float] = None


class DAGExecutor:
    """Execute task graphs in topological order with concurrent ready nodes."""

    def __init__(self, engine: Optional[AsyncTaskEngine] = None) -> None:
        self.engine = engine or AsyncTaskEngine(max_workers=4)

    async def execute(self, graph: nx.DiGraph, input_payload: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        if not isinstance(graph, nx.DiGraph):
            raise TypeError("graph must be a networkx.DiGraph")
        if not nx.is_directed_acyclic_graph(graph):
            raise ValueError("DAG graph must be acyclic")

        results: Dict[str, Any] = {}
        pending = set(graph.nodes)
        scheduled: Dict[str, str] = {}

        while pending:
            ready = [
                node_id
                for node_id in pending
                if all(dep in results for dep in graph.predecessors(node_id))
            ]
            if not ready:
                if not pending:
                    break
                raise RuntimeError("Graph contains unresolved dependencies or a cycle.")

            scheduled = {}
            for node_id in ready:
                node_data = graph.nodes[node_id]
                func = node_data.get("func")
                if func is None:
                    raise ValueError(f"Node {node_id} is missing a callable function")

                dependency_values = [results[dep] for dep in sorted(graph.predecessors(node_id))]
                prepared_args = tuple(node_data.get("args", ()))
                prepared_kwargs = dict(node_data.get("kwargs", {}))

                if node_data.get("call_style") == "mapping":
                    prepared_args = ()
                    prepared_kwargs = {**prepared_kwargs, **{dep: results[dep] for dep in graph.predecessors(node_id)}}
                else:
                    prepared_args = prepared_args + tuple(dependency_values)

                task = TaskSpec(
                    func=func,
                    task_id=f"{node_id}-{uuid.uuid4().hex[:8]}",
                    args=prepared_args,
                    kwargs=prepared_kwargs,
                    max_retries=int(node_data.get("retries", 3)),
                    retry_backoff=float(node_data.get("retry_backoff", 0.25)),
                    timeout=node_data.get("timeout"),
                    metadata={"node_id": node_id, "dependencies": list(graph.predecessors(node_id))},
                )
                task_id = await self.engine.submit(task)
                scheduled[node_id] = task_id

            if not scheduled:
                break

            await asyncio.gather(*(self.engine.wait_for_task(task_id) for task_id in scheduled.values()))

            for node_id, task_id in scheduled.items():
                task_result = self.engine.get_task_result(task_id)
                if task_result.state != TaskState.SUCCEEDED:
                    raise RuntimeError(f"DAG node {node_id} failed: {task_result.error}")
                results[node_id] = task_result.result
                pending.discard(node_id)

        if input_payload:
            results["input"] = dict(input_payload)

        return results

    def build_graph(self, nodes: Iterable[DAGNode]) -> nx.DiGraph:
        graph = nx.DiGraph()
        for node in nodes:
            graph.add_node(
                node.node_id,
                func=node.func,
                args=node.args,
                kwargs=node.kwargs,
                retries=node.retries,
                retry_backoff=node.retry_backoff,
                timeout=node.timeout,
                call_style="mapping" if node.depends_on else "positional",
            )
            for dependency in node.depends_on:
                graph.add_edge(dependency, node.node_id)
        return graph
