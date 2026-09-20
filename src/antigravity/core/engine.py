from __future__ import annotations

import asyncio
import inspect
import logging
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("antigravity.engine")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class TaskState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class TaskSpec:
    func: Callable[..., Any]
    task_id: str = field(default_factory=lambda: f"task-{uuid.uuid4().hex[:12]}")
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    max_retries: int = 3
    retry_backoff: float = 0.25
    timeout: Optional[float] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskResult:
    task_id: str
    state: TaskState
    result: Any = None
    error: Optional[str] = None
    attempts: int = 0
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class AsyncTaskEngine:
    """Asynchronous task queue with retry, concurrency and result tracking."""

    def __init__(
        self,
        max_workers: int = 4,
        queue_size: int = 1024,
        retry_backoff: float = 0.25,
    ) -> None:
        self.max_workers = max_workers
        self.queue = asyncio.Queue(maxsize=queue_size)
        self.retry_backoff = retry_backoff
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._task_futures: Dict[str, asyncio.Future[TaskResult]] = {}
        self._task_specs: Dict[str, TaskSpec] = {}
        self._run = True
        self._started = False
        self._workers: list[asyncio.Task[None]] = []
        self.metrics = {
            "submitted": 0,
            "completed": 0,
            "failed": 0,
            "retries": 0,
            "active_tasks": 0,
        }

    async def _ensure_started(self) -> None:
        if self._started:
            return
        loop = asyncio.get_running_loop()
        self._workers = [
            loop.create_task(self._worker(worker_index))
            for worker_index in range(self.max_workers)
        ]
        self._started = True

    async def submit(self, task: TaskSpec) -> str:
        await self._ensure_started()
        task_id = task.task_id
        self._task_specs[task_id] = task
        loop = asyncio.get_running_loop()
        future: asyncio.Future[TaskResult] = loop.create_future()
        self._task_futures[task_id] = future
        self.metrics["submitted"] += 1
        self.metrics["active_tasks"] = len(self._task_futures)
        await self.queue.put(task)
        logger.info("Queued task %s", task_id)
        return task_id

    async def wait_for_task(self, task_id: str) -> TaskResult:
        future = self._task_futures.get(task_id)
        if future is None:
            raise KeyError(f"Unknown task_id: {task_id}")
        return await future

    def get_task_result(self, task_id: str) -> TaskResult:
        future = self._task_futures.get(task_id)
        if future is None or not future.done():
            raise KeyError(f"Task {task_id} is not completed yet")
        return future.result()

    def get_task_status(self, task_id: str) -> TaskState:
        task_result = self._task_futures.get(task_id)
        if task_result is None:
            raise KeyError(f"Unknown task_id: {task_id}")
        if task_result.done():
            return task_result.result().state
        return TaskState.RUNNING

    def metrics_snapshot(self) -> Dict[str, Any]:
        return {
            "submitted": self.metrics["submitted"],
            "completed": self.metrics["completed"],
            "failed": self.metrics["failed"],
            "retries": self.metrics["retries"],
            "active_tasks": self.metrics["active_tasks"],
            "queue_depth": self.queue.qsize(),
            "max_workers": self.max_workers,
        }

    async def _worker(self, worker_index: int) -> None:
        while self._run:
            task = await self.queue.get()
            if task is None:
                self.queue.task_done()
                break
            try:
                await self._run_task(task)
            finally:
                self.queue.task_done()
            logger.debug("Worker %s finished task %s", worker_index, task.task_id)

    async def _run_task(self, task: TaskSpec) -> None:
        future = self._task_futures.get(task.task_id)
        if future is None:
            raise KeyError(f"Task future missing for {task.task_id}")

        attempts = 0
        while True:
            attempts += 1
            started_at = utc_now()
            try:
                logger.info("Starting task %s attempt %s", task.task_id, attempts)
                result = await self._call_callable(task)
                task_result = TaskResult(
                    task_id=task.task_id,
                    state=TaskState.SUCCEEDED,
                    result=result,
                    attempts=attempts,
                    started_at=started_at,
                    finished_at=utc_now(),
                    metadata=task.metadata,
                )
                future.set_result(task_result)
                self.metrics["completed"] += 1
                self.metrics["active_tasks"] = max(0, self.metrics["active_tasks"] - 1)
                return
            except asyncio.TimeoutError:
                error_message = f"Task {task.task_id} timed out after {task.timeout}s"
                logger.warning(error_message)
                if attempts <= task.max_retries:
                    self.metrics["retries"] += 1
                    await self._sleep_with_backoff(task, attempts)
                    continue
                self._finalize_failure(future, task, attempts, started_at, error_message)
                return
            except Exception as exc:  # pragma: no cover - exercised via tests
                error_message = f"Task {task.task_id} failed on attempt {attempts}: {exc}\n{traceback.format_exc()}"
                logger.exception(error_message)
                if attempts <= task.max_retries:
                    self.metrics["retries"] += 1
                    await self._sleep_with_backoff(task, attempts)
                    continue
                self._finalize_failure(future, task, attempts, started_at, error_message)
                return

    async def _call_callable(self, task: TaskSpec) -> Any:
        if task.timeout is not None:
            return await asyncio.wait_for(self._invoke_callable(task), timeout=task.timeout)
        return await self._invoke_callable(task)

    async def _invoke_callable(self, task: TaskSpec) -> Any:
        if asyncio.iscoroutinefunction(task.func):
            return await task.func(*task.args, **task.kwargs)

        loop = asyncio.get_running_loop()

        def invoke_sync() -> Any:
            return task.func(*task.args, **task.kwargs)

        sync_result = await loop.run_in_executor(self._executor, invoke_sync)
        if inspect.isawaitable(sync_result):
            return await sync_result
        return sync_result

    async def _sleep_with_backoff(self, task: TaskSpec, attempt_number: int) -> None:
        delay = self.retry_backoff + (task.retry_backoff * (2 ** (attempt_number - 1)))
        logger.warning("Retrying task %s in %.2f seconds", task.task_id, delay)
        await asyncio.sleep(delay)

    def _finalize_failure(
        self,
        future: asyncio.Future[TaskResult],
        task: TaskSpec,
        attempts: int,
        started_at: str,
        error_message: str,
    ) -> None:
        task_result = TaskResult(
            task_id=task.task_id,
            state=TaskState.FAILED,
            error=error_message,
            attempts=attempts,
            started_at=started_at,
            finished_at=utc_now(),
            metadata=task.metadata,
        )
        future.set_result(task_result)
        self.metrics["failed"] += 1
        self.metrics["active_tasks"] = max(0, self.metrics["active_tasks"] - 1)

    async def close(self) -> None:
        self._run = False
        if not self._started:
            self._executor.shutdown(wait=True)
            logger.info("Task engine shut down without workers")
            return
        for _ in range(self.max_workers):
            await self.queue.put(None)
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._executor.shutdown(wait=True)
        logger.info("Task engine shut down")
