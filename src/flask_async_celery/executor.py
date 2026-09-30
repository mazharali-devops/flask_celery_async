from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Coroutine
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class TaskInfo:
    task_id: str
    task_name: str | None
    started_at: float
    finished_at: float | None = None
    state: str = "RUNNING"
    exception: str | None = None

    @property
    def duration(self) -> float:
        end = self.finished_at or time.monotonic()
        return end - self.started_at


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
            slow_task_threshold: float = 30.0,

    ) -> None:
        if max_tasks < 1:
            raise ValueError("max_tasks must be greater than zero")

        if slow_task_threshold < 0:
            raise ValueError(
                "slow_task_threshold must be greater than or equal to zero")
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

        self._tasks: dict[str, TaskInfo] = {}
        self._tasks_lock = threading.Lock()
        self._cancel_reasons: dict[str, Any] = {}
        self._asyncio_tasks: dict[str, asyncio.Task[Any]] = {}

        self._completed = 0
        self._failed = 0
        self._cancelled = 0
        self._total_duration = 0.0

        self.slow_task_threshold = float(slow_task_threshold)

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

        loop.call_soon(self._started.set)

        logger.info(
            "AsyncIO event loop starting: max_tasks=%s",
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
            *,
            task_id: str | None = None,
            task_name: str | None = None,
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
        if task_id is None:
            task_id = f"async-{id(coroutine)}"

        try:
            return asyncio.run_coroutine_threadsafe(
                self._execute(
                    coroutine,
                    task_id=task_id,
                    task_name=task_name,
                ),
                loop,
            )

        except BaseException:
            self._close_coroutine(coroutine)
            raise

    async def _execute(
            self,
            coroutine,
            *,
            task_id: str,
            task_name: str | None,
    ):
        semaphore = self._semaphore

        if semaphore is None:
            self._close_coroutine(coroutine)
            raise RuntimeError("AsyncIO executor is not initialized")

        current_task = asyncio.current_task()

        info = TaskInfo(
            task_id=task_id,
            task_name=task_name,
            started_at=0.0,
        )

        with self._tasks_lock:
            self._tasks[task_id] = info

            if current_task is not None:
                self._asyncio_tasks[task_id] = current_task

        acquired = False

        try:
            await semaphore.acquire()

            acquired = True
            info.started_at = time.monotonic()

            self._increment_running()

            try:
                result = await coroutine

            except asyncio.CancelledError:
                info.state = "CANCELLED"
                raise

            except BaseException as exc:
                info.state = "FAILURE"
                info.exception = f"{type(exc).__name__}: {exc}"
                raise

            else:
                info.state = "SUCCESS"
                return result

            finally:
                info.finished_at = time.monotonic()

                with self._tasks_lock:
                    self._total_duration += info.duration

                    if info.state == "SUCCESS":
                        self._completed += 1
                    elif info.state == "FAILURE":
                        self._failed += 1
                    elif info.state == "CANCELLED":
                        self._cancelled += 1

                self._decrement_running()

        except asyncio.CancelledError:
            # Cancellation can happen while waiting for the semaphore,
            # before the coroutine itself has started.
            if not acquired:
                info.state = "CANCELLED"
                with self._tasks_lock:
                    self._cancelled += 1

            raise

        finally:
            with self._tasks_lock:
                self._tasks.pop(task_id, None)
                self._asyncio_tasks.pop(task_id, None)

            if not acquired:
                self._close_coroutine(coroutine)
            else:
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

    def running_tasks(self) -> list[dict[str, Any]]:
        with self._tasks_lock:
            return [
                {
                    "task_id": info.task_id,
                    "task_name": info.task_name,
                    "state": info.state,
                    "started_at": info.started_at,
                    "duration": info.duration,
                    "exception": info.exception,
                }
                for info in self._tasks.values()
            ]

    def stats(self) -> dict[str, Any]:
        with self._tasks_lock:
            completed = self._completed
            failed = self._failed
            cancelled = self._cancelled
            total_duration = self._total_duration

            long_running = sum(
                1
                for info in self._tasks.values()
                if info.duration >= self.slow_task_threshold
            )

        finished = completed + failed + cancelled

        return {
            "max_tasks": self.max_tasks,
            "running": self.running,
            "available": self.available,
            "completed": completed,
            "failed": failed,
            "cancelled": cancelled,
            "total": finished,
            "average_duration": (
                total_duration / finished if finished else 0.0
            ),
            "slow_task_threshold": self.slow_task_threshold,
            "long_running": long_running,
            "event_loop_running": self.is_running,
            "stopping": self.is_stopping,
        }

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        with self._tasks_lock:
            info = self._tasks.get(task_id)

            if info is None:
                return None

            return {
                "task_id": info.task_id,
                "task_name": info.task_name,
                "state": info.state,
                "started_at": info.started_at,
                "duration": info.duration,
                "exception": info.exception,
            }

    def long_running_tasks(self) -> list[dict[str, Any]]:
        with self._tasks_lock:
            return [
                {
                    "task_id": info.task_id,
                    "task_name": info.task_name,
                    "state": info.state,
                    "started_at": info.started_at,
                    "duration": info.duration,
                    "exception": info.exception,
                }
                for info in self._tasks.values()
                if info.duration >= self.slow_task_threshold
            ]

    def cancel_task(
            self,
            task_id: str,
            *,
            reason: Any = None,
    ) -> bool:
        with self._tasks_lock:
            task = self._asyncio_tasks.get(task_id)

        if task is None:
            return False

        loop = self.loop

        if loop is None or not loop.is_running():
            return False

        if reason is not None:
            with self._tasks_lock:
                self._cancel_reasons[task_id] = reason

        try:
            loop.call_soon_threadsafe(task.cancel)
        except RuntimeError:
            if reason is not None:
                with self._tasks_lock:
                    self._cancel_reasons.pop(task_id, None)
            return False

        return True

    def get_cancel_reason(self, task_id: str) -> Any:
        with self._tasks_lock:
            return self._cancel_reasons.get(task_id)

    def clear_cancel_reason(self, task_id: str) -> None:
        with self._tasks_lock:
            self._cancel_reasons.pop(task_id, None)
