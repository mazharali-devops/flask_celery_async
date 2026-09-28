from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Coroutine
from concurrent.futures import Future
from typing import Any

logger = logging.getLogger(__name__)


class AsyncExecutor:
    """
    Persistent asyncio executor.

    One instance owns one asyncio event loop running in one
    dedicated thread.

    The executor provides bounded asynchronous concurrency.
    It is not a Celery worker pool.
    """

    def __init__(
            self,
            max_tasks: int = 20,
            thread_name: str = "celery-asyncio",
    ) -> None:
        if max_tasks < 1:
            raise ValueError("max_tasks must be greater than zero")

        self.max_tasks = max_tasks
        self.thread_name = thread_name

        self.loop: asyncio.AbstractEventLoop | None = None
        self.thread: threading.Thread | None = None

        self._started = threading.Event()
        self._stopping = threading.Event()
        self._stopped = threading.Event()

        self._semaphore: asyncio.Semaphore | None = None

        self._running = 0
        self._running_lock = threading.Lock()

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    @property
    def running(self) -> int:
        """
        Number of coroutines currently executing.

        This counts only tasks that have acquired the concurrency
        semaphore and are actively executing.
        """
        with self._running_lock:
            return self._running

    @property
    def available(self) -> int:
        """
        Number of currently available execution slots.
        """
        return max(0, self.max_tasks - self.running)

    @property
    def is_running(self) -> bool:
        """
        True when the asyncio event loop is running and the executor
        has not entered shutdown.
        """
        return (
                self.loop is not None
                and self.loop.is_running()
                and not self._stopping.is_set()
        )

    @property
    def is_stopping(self) -> bool:
        """
        True after shutdown has started.
        """
        return self._stopping.is_set()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return

        self._started.clear()
        self._stopped.clear()
        self._stopping.clear()

        self.thread = threading.Thread(
            target=self._run_loop,
            name=self.thread_name,
            daemon=True,
        )

        self.thread.start()

        if not self._started.wait(timeout=10):
            raise RuntimeError(
                "AsyncIO executor failed to start within 10 seconds"
            )

        logger.info(
            "AsyncIO executor started: max_tasks=%s thread=%s",
            self.max_tasks,
            self.thread.name,
        )

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()

        self.loop = loop

        asyncio.set_event_loop(loop)

        self._semaphore = asyncio.Semaphore(self.max_tasks)

        self._started.set()

        logger.info(
            "AsyncIO event loop started: max_tasks=%s",
            self.max_tasks,
        )

        try:
            loop.run_forever()
        finally:
            self._shutdown_loop(loop)

    def _shutdown_loop(
            self,
            loop: asyncio.AbstractEventLoop,
    ) -> None:
        logger.info("Shutting down AsyncIO event loop")

        try:
            pending = asyncio.all_tasks(loop)

            if pending:
                logger.info(
                    "Cancelling %s pending asyncio task(s)",
                    len(pending),
                )

                for task in pending:
                    task.cancel()

                loop.run_until_complete(
                    asyncio.gather(
                        *pending,
                        return_exceptions=True,
                    )
                )

        except Exception:
            logger.exception(
                "Error while shutting down AsyncIO tasks"
            )

        finally:
            try:
                loop.close()
            except Exception:
                logger.exception(
                    "Error while closing AsyncIO event loop"
                )

            self.loop = None
            self._semaphore = None

            self._stopped.set()

            logger.info("AsyncIO event loop stopped")

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------

    def submit(
            self,
            coroutine: Coroutine[Any, Any, Any],
    ) -> Future:
        """
        Submit a coroutine to the persistent asyncio event loop.

        The returned Future is a concurrent.futures.Future and can be
        waited on safely from Celery's synchronous execution context.
        """
        if not self.is_running:
            self._close_coroutine(coroutine)

            raise RuntimeError(
                "AsyncIO executor is not running"
            )

        loop = self.loop

        if loop is None:
            self._close_coroutine(coroutine)

            raise RuntimeError(
                "AsyncIO executor event loop is unavailable"
            )

        try:
            return asyncio.run_coroutine_threadsafe(
                self._execute(coroutine),
                loop,
            )

        except BaseException:
            self._close_coroutine(coroutine)
            raise

    async def _execute(
            self,
            coroutine: Coroutine[Any, Any, Any],
    ) -> Any:
        """
        Execute one coroutine while respecting the concurrency limit.

        The execution slot is always released through finally,
        regardless of success, exception, or cancellation.
        """
        semaphore = self._semaphore

        if semaphore is None:
            self._close_coroutine(coroutine)

            raise RuntimeError(
                "AsyncIO semaphore is not initialized"
            )

        acquired = False

        try:
            await semaphore.acquire()
            acquired = True

            self._increment_running()

            try:
                return await coroutine

            finally:
                self._decrement_running()

        finally:
            if acquired:
                semaphore.release()

    # ------------------------------------------------------------------
    # Running task accounting
    # ------------------------------------------------------------------

    def _increment_running(self) -> None:
        with self._running_lock:
            self._running += 1

            logger.debug(
                "AsyncIO tasks: %s/%s",
                self._running,
                self.max_tasks,
            )

    def _decrement_running(self) -> None:
        with self._running_lock:
            if self._running <= 0:
                logger.error(
                    "AsyncIO running counter underflow"
                )
                self._running = 0
                return

            self._running -= 1

            logger.debug(
                "AsyncIO tasks: %s/%s",
                self._running,
                self.max_tasks,
            )

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(
            self,
            wait: bool = True,
            timeout: float = 10,
    ) -> None:
        self._stopping.set()

        loop = self.loop

        if loop is None:
            self._stopped.set()
            return

        if not loop.is_running():
            self._stopped.set()
            return

        logger.info("Stopping AsyncIO executor")

        try:
            loop.call_soon_threadsafe(loop.stop)
        except RuntimeError:
            logger.debug(
                "AsyncIO event loop already stopped",
                exc_info=True,
            )

        if wait:
            if self.thread:
                self.thread.join(timeout=timeout)

                if self.thread.is_alive():
                    logger.warning(
                        "AsyncIO thread did not stop within %.1f seconds",
                        timeout,
                    )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _close_coroutine(
            coroutine: Coroutine[Any, Any, Any],
    ) -> None:
        """
        Close a coroutine that could not be submitted.

        This prevents 'coroutine was never awaited' warnings when
        submission fails before the coroutine reaches the event loop.
        """
        try:
            coroutine.close()
        except Exception:
            logger.exception(
                "Failed to close coroutine after submission failure"
            )
