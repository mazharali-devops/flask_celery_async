from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import Future
from typing import Any
from collections.abc import Coroutine


logger = logging.getLogger(__name__)


class AsyncExecutor:
    """
    Persistent asyncio executor.

    One instance runs one asyncio event loop inside one
    dedicated thread.

    The executor itself is NOT a Celery worker pool.
    It only knows how to execute coroutines concurrently.
    """

    def __init__(
        self,
        max_tasks: int = 20,
        thread_name: str = "celery-asyncio",
    ) -> None:

        if max_tasks < 1:
            raise ValueError(
                "max_tasks must be greater than zero"
            )

        self.max_tasks = max_tasks
        self.thread_name = thread_name

        self.loop: asyncio.AbstractEventLoop | None = None

        self.thread: threading.Thread | None = None

        self._started = threading.Event()

        self._stopping = threading.Event()

        self._semaphore: asyncio.Semaphore | None = None

        self._running = 0

        self._running_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def running(self) -> int:
        with self._running_lock:
            return self._running

    @property
    def available(self) -> int:
        return max(
            0,
            self.max_tasks - self.running,
        )

    @property
    def is_running(self) -> bool:
        return (
            self.loop is not None
            and self.loop.is_running()
            and not self._stopping.is_set()
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """
        Start the dedicated asyncio thread.
        """

        if self.thread and self.thread.is_alive():
            return

        self._started.clear()
        self._stopping.clear()

        self.thread = threading.Thread(
            target=self._run_loop,
            name=self.thread_name,
            daemon=True,
        )

        self.thread.start()

        if not self._started.wait(timeout=10):
            raise RuntimeError(
                "AsyncIO event loop failed to start"
            )

        logger.info(
            "AsyncIO executor started: "
            "max_tasks=%s thread=%s",
            self.max_tasks,
            self.thread.name,
        )

    def _run_loop(self) -> None:
        """
        Runs inside the dedicated thread.
        """

        loop = asyncio.new_event_loop()

        self.loop = loop

        asyncio.set_event_loop(loop)

        self._semaphore = asyncio.Semaphore(
            self.max_tasks
        )

        self._started.set()

        logger.info(
            "AsyncIO event loop started"
        )

        try:
            loop.run_forever()

        finally:
            self._shutdown_loop(loop)

    def _shutdown_loop(
        self,
        loop: asyncio.AbstractEventLoop,
    ) -> None:

        logger.info(
            "Shutting down AsyncIO event loop"
        )

        try:
            pending = asyncio.all_tasks(loop)

            for task in pending:
                task.cancel()

            if pending:
                loop.run_until_complete(
                    asyncio.gather(
                        *pending,
                        return_exceptions=True,
                    )
                )

        except Exception:
            logger.exception(
                "Error while shutting down "
                "AsyncIO tasks"
            )

        finally:
            loop.close()

            logger.info(
                "AsyncIO event loop stopped"
            )

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------

    def submit(
        self,
        coroutine: Coroutine[Any, Any, Any],
    ) -> Future:
        """
        Submit a coroutine to the persistent event loop.

        Returns a concurrent.futures.Future.
        """

        if not self.is_running:
            raise RuntimeError(
                "AsyncIO executor is not running"
            )

        return asyncio.run_coroutine_threadsafe(
            self._execute(coroutine),
            self.loop,
        )

    async def _execute(
        self,
        coroutine: Coroutine[Any, Any, Any],
    ) -> Any:

        if self._semaphore is None:
            raise RuntimeError(
                "AsyncIO semaphore is not initialized"
            )

        async with self._semaphore:

            self._increment_running()

            try:
                return await coroutine

            finally:
                self._decrement_running()

    # ------------------------------------------------------------------
    # Counters
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

        if self.loop is None:
            return

        if not self.loop.is_running():
            return

        self._stopping.set()

        logger.info(
            "Stopping AsyncIO executor"
        )

        self.loop.call_soon_threadsafe(
            self.loop.stop
        )

        if wait and self.thread:

            self.thread.join(
                timeout=timeout
            )

            if self.thread.is_alive():

                logger.warning(
                    "AsyncIO thread did not stop "
                    "within %.1f seconds",
                    timeout,
                )