from __future__ import annotations

import logging

from celery import bootsteps

logger = logging.getLogger(__name__)


class AsyncIOBootStep(bootsteps.StartStopStep):
    """
    Worker bootstep for the Flask Async Celery integration.

    This bootstep exposes the asyncio executor owned by AsyncIOPool
    through the Celery worker so other worker components can inspect
    its state.
    """

    requires = {"celery.worker.components:Pool"}

    def __init__(self, worker, **kwargs):
        self.worker = worker
        super().__init__(worker, **kwargs)

    def start(self, worker) -> None:
        """Called when the Celery worker starts."""

        pool = getattr(worker, "pool", None)

        logger.info(
            "AsyncIO worker bootstep started: pool=%s",
            type(pool).__name__ if pool else None,
        )

        # The custom pool owns the AsyncExecutor.
        #
        # Expose it on the worker so other bootsteps/components
        # can inspect its state without importing the pool directly.
        if pool is not None and hasattr(pool, "async_executor"):
            worker.async_executor = pool.async_executor

        worker.asyncio_enabled = True

    def stop(self, worker) -> None:
        """Called during normal worker shutdown."""

        logger.info("AsyncIO worker bootstep stopping")

        worker.asyncio_enabled = False

    def terminate(self, worker) -> None:
        """Called during forced worker termination."""

        logger.info("AsyncIO worker bootstep terminating")

        worker.asyncio_enabled = False

    def info(self, worker):
        executor = getattr(worker, "async_executor", None)

        if executor is None:
            return {
                "asyncio-enabled": False,
            }

        stats = executor.stats()

        return {
            "asyncio-enabled": True,
            "asyncio-running": stats["running"],
            "asyncio-available": stats["available"],
            "asyncio-max-tasks": stats["max_tasks"],
            "asyncio-completed": stats["completed"],
            "asyncio-failed": stats["failed"],
            "asyncio-cancelled": stats["cancelled"],
            "asyncio-total": stats["total"],
            "asyncio-average-duration": stats["average_duration"],
            "asyncio-slow-task-threshold": stats["slow_task_threshold"],
            "asyncio-long-running": stats["long_running"],
            "asyncio-long-running-tasks": executor.long_running_tasks(),
            "asyncio-event-loop-running": stats["event_loop_running"],
            "asyncio-stopping": stats["stopping"],
        }
