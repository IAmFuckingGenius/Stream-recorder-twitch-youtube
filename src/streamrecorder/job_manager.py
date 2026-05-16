"""Async job queues and per-target upload scheduling."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Optional, TypeVar

from .logger import get_logger


T = TypeVar("T")

JobCallable = Callable[[], Awaitable[T]]


@dataclass
class JobQueueStats:
    """Runtime stats for one logical job queue."""

    name: str
    concurrency: int
    active: int = 0
    queued: int = 0
    completed: int = 0
    failed: int = 0


@dataclass
class JobManager:
    """Small in-process queue manager with named concurrency limits."""

    limits: Dict[str, int]
    _queues: Dict[str, asyncio.Semaphore] = field(init=False, default_factory=dict)
    _stats: Dict[str, JobQueueStats] = field(init=False, default_factory=dict)
    _lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)
    _logger: Any = field(init=False, default_factory=lambda: get_logger("job_manager"))

    def __post_init__(self) -> None:
        for name, limit in self.limits.items():
            self._ensure_queue(name, limit)

    def _ensure_queue(self, name: str, limit: int = 1) -> None:
        limit = max(1, int(limit or 1))
        if name not in self._queues:
            self._queues[name] = asyncio.Semaphore(limit)
            self._stats[name] = JobQueueStats(name=name, concurrency=limit)

    async def run(self, queue_name: str, job_name: str, job: JobCallable[T]) -> T:
        """Run a coroutine under a named queue limit."""
        self._ensure_queue(queue_name)
        semaphore = self._queues[queue_name]
        stats = self._stats[queue_name]

        async with self._lock:
            stats.queued += 1

        async with semaphore:
            async with self._lock:
                stats.queued = max(0, stats.queued - 1)
                stats.active += 1
            started = datetime.now()
            self._logger.debug("Job started: %s/%s", queue_name, job_name)
            try:
                result = await job()
            except Exception:
                async with self._lock:
                    stats.failed += 1
                raise
            else:
                async with self._lock:
                    stats.completed += 1
                return result
            finally:
                elapsed = (datetime.now() - started).total_seconds()
                async with self._lock:
                    stats.active = max(0, stats.active - 1)
                self._logger.debug("Job finished: %s/%s in %.1fs", queue_name, job_name, elapsed)

    async def snapshot(self) -> Dict[str, JobQueueStats]:
        """Return a copy of current queue stats."""
        async with self._lock:
            return {
                name: JobQueueStats(**stats.__dict__)
                for name, stats in self._stats.items()
            }


class TargetUploadScheduler:
    """Serialize upload pipelines per target while respecting target parallelism."""

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Semaphore] = {}
        self._limits: dict[str, int] = {}
        self._active: dict[str, int] = defaultdict(int)
        self._queued: dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    async def run(self, target_id: str, concurrency: int, job: JobCallable[T]) -> T:
        target_id = target_id or "default"
        concurrency = max(1, int(concurrency or 1))
        async with self._lock:
            current_limit = self._limits.get(target_id)
            if target_id not in self._locks or (current_limit != concurrency and self._active[target_id] == 0):
                self._locks[target_id] = asyncio.Semaphore(concurrency)
                self._limits[target_id] = concurrency
            self._queued[target_id] += 1
            semaphore = self._locks[target_id]

        async with semaphore:
            async with self._lock:
                self._queued[target_id] = max(0, self._queued[target_id] - 1)
                self._active[target_id] += 1
            try:
                return await job()
            finally:
                async with self._lock:
                    self._active[target_id] = max(0, self._active[target_id] - 1)

    async def snapshot(self) -> dict[str, dict[str, int]]:
        async with self._lock:
            keys = sorted(set(self._active) | set(self._queued) | set(self._locks))
            return {
                key: {
                    "active": self._active[key],
                    "queued": self._queued[key],
                    "limit": self._limits.get(key, 1),
                }
                for key in keys
            }
