from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import duckdb  # type: ignore
except Exception:  # pragma: no cover - duckdb optional
    duckdb = None


class TaskStorageAdapter:
    """Async-friendly persistence adapter for task metadata and runtime state."""

    def __init__(self, db_path: str | Path = "antigravity.db", backend: str = "sqlite") -> None:
        self.db_path = str(Path(db_path))
        self.backend = backend.lower()
        self._connection: Any = None
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        await asyncio.to_thread(self._init_database)
        self._initialized = True

    def _init_database(self) -> None:
        if self.backend == "duckdb" and duckdb is not None:
            self._connection = duckdb.connect(self.db_path)
        else:
            self._connection = sqlite3.connect(self.db_path, check_same_thread=False)

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                payload TEXT DEFAULT '{}',
                result TEXT DEFAULT '{}',
                error TEXT,
                attempts INTEGER DEFAULT 0,
                duration_ms REAL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                metrics TEXT DEFAULT '{}'
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS task_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        self._connection.commit()

    async def upsert_task(
        self,
        task_id: str,
        status: str,
        payload: Optional[Dict[str, Any]] = None,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        attempts: int = 0,
        duration_ms: float = 0.0,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        await self.initialize()
        payload_json = json.dumps(payload or {}, sort_keys=True)
        result_json = json.dumps(result or {}, sort_keys=True)
        metrics_json = json.dumps(metrics or {}, sort_keys=True)
        timestamp = __import__("datetime").datetime.utcnow().isoformat(timespec="seconds") + "Z"

        await asyncio.to_thread(
            self._persist_task,
            task_id,
            status,
            payload_json,
            result_json,
            error,
            attempts,
            duration_ms,
            metrics_json,
            timestamp,
        )

    def _persist_task(
        self,
        task_id: str,
        status: str,
        payload_json: str,
        result_json: str,
        error: Optional[str],
        attempts: int,
        duration_ms: float,
        metrics_json: str,
        timestamp: str,
    ) -> None:
        self._connection.execute(
            """
            INSERT INTO tasks (task_id, status, payload, result, error, attempts, duration_ms, created_at, updated_at, metrics)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                status = excluded.status,
                payload = excluded.payload,
                result = excluded.result,
                error = excluded.error,
                attempts = excluded.attempts,
                duration_ms = excluded.duration_ms,
                updated_at = excluded.updated_at,
                metrics = excluded.metrics
            """,
            (
                task_id,
                status,
                payload_json,
                result_json,
                error,
                attempts,
                duration_ms,
                timestamp,
                timestamp,
                metrics_json,
            ),
        )
        self._connection.commit()

    async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        await self.initialize()
        return await asyncio.to_thread(self._fetch_task, task_id)

    def _fetch_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        row = self._connection.execute(
            "SELECT task_id, status, payload, result, error, attempts, duration_ms, created_at, updated_at, metrics FROM tasks WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if row is None:
            return None

        columns = [
            "task_id",
            "status",
            "payload",
            "result",
            "error",
            "attempts",
            "duration_ms",
            "created_at",
            "updated_at",
            "metrics",
        ]
        values = dict(zip(columns, row))
        for key in ("payload", "result", "metrics"):
            if values.get(key):
                try:
                    values[key] = json.loads(values[key])
                except (TypeError, json.JSONDecodeError):
                    values[key] = {}
        return values

    async def list_tasks(self) -> List[Dict[str, Any]]:
        await self.initialize()
        return await asyncio.to_thread(self._list_tasks)

    def _list_tasks(self) -> List[Dict[str, Any]]:
        rows = self._connection.execute(
            "SELECT task_id, status, payload, result, error, attempts, duration_ms, created_at, updated_at, metrics FROM tasks ORDER BY created_at DESC"
        ).fetchall()
        items = []
        columns = [
            "task_id",
            "status",
            "payload",
            "result",
            "error",
            "attempts",
            "duration_ms",
            "created_at",
            "updated_at",
            "metrics",
        ]
        for row in rows:
            item = dict(zip(columns, row))
            for key in ("payload", "result", "metrics"):
                if item.get(key):
                    try:
                        item[key] = json.loads(item[key])
                    except (TypeError, json.JSONDecodeError):
                        item[key] = {}
            items.append(item)
        return items

    async def log_event(self, task_id: str, level: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        await self.initialize()
        await asyncio.to_thread(self._write_log, task_id, level, message, details)

    def _write_log(self, task_id: str, level: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        payload = json.dumps(details or {}, sort_keys=True)
        timestamp = __import__("datetime").datetime.utcnow().isoformat(timespec="seconds") + "Z"
        self._connection.execute(
            "INSERT INTO task_logs (task_id, level, message, details, created_at) VALUES (?, ?, ?, ?, ?)",
            (task_id, level, message, payload, timestamp),
        )
        self._connection.commit()

    async def close(self) -> None:
        if self._connection is None:
            return
        await asyncio.to_thread(self._connection.close)
        self._connection = None
        self._initialized = False
