from __future__ import annotations

import logging

from celery import bootsteps

logger = logging.getLogger(__name__)


class AsyncIOBootStep(bootsteps.StartStopStep):
    """
    Worker bootstep for the Flask Async Celery integration.

    The Celery Pool bootstep is required so this component starts
    after the worker pool has been created.

    The actual asyncio event loop is owned by AsyncIOPool/
    AsyncExecutor. This bootstep provides the worker-level hook
    that we will use for lifecycle and backpressure handling.
    """

    requires = {"celery.worker.components:Pool"}

    def __init__(self, worker, **kwargs):
        self.worker = worker
        super().__init__(worker, **kwargs)

    def start(self, worker) -> None:
        """
        Called when the Celery worker starts.
        """

        pool = getattr(worker, "pool", None)

        logger.info(
            "AsyncIO worker bootstep started: pool=%s",
            type(pool).__name__ if pool else None,
        )

        # The custom pool owns the AsyncExecutor.
        #
        # Keep a reference on the worker so other bootsteps/components
        # can access it without importing the pool implementation.
        if pool is not None and hasattr(pool, "executor"):
            worker.async_executor = pool.async_executor

        worker.asyncio_enabled = True

    def stop(self, worker) -> None:
        """
        Called during normal worker shutdown.
        """

        logger.info("AsyncIO worker bootstep stopping")

        worker.asyncio_enabled = False

    def terminate(self, worker) -> None:
        """
        Called during forced worker termination.
        """

        logger.info("AsyncIO worker bootstep terminating")

        worker.asyncio_enabled = False

    def info(self, worker):
        """
        Expose basic information through worker inspection.
        """

        executor = getattr(worker, "async_executor", None)

        if executor is None:
            return {
                "asyncio-enabled": False,
            }

        return {
            "asyncio-enabled": True,
            "asyncio-running": executor.running,
            "asyncio-available": executor.available,
            "asyncio-max-tasks": executor.max_tasks,
        }