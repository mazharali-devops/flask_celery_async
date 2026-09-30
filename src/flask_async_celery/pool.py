from __future__ import annotations

import inspect
import logging
import os
import threading
import time
from concurrent.futures import CancelledError
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any, Callable

from celery.concurrency.base import BasePool

from .executor import AsyncExecutor
from .task import reset_async_executor, set_async_executor

logger = logging.getLogger(__name__)


class ApplyResult:
    def __init__(
            self,
            future: Future,
            terminate_callback: Callable[[int | None], bool] | None = None,
    ) -> None:
        self.f = future
        self.get = future.result
        self._terminate_callback = terminate_callback

    def wait(self, timeout: float | None = None) -> None:
        wait([self.f], timeout)

    def ready(self) -> bool:
        return self.f.done()

    def successful(self) -> bool:
        if not self.f.done():
            return False
        return self.f.exception() is None

    def terminate(self, signal=None) -> bool:
        if self._terminate_callback is None:
            return False

        return self._terminate_callback(signal)


class AsyncIOPool(BasePool):
    """
    Celery execution pool for asyncio tasks.

    There are deliberately TWO execution layers:

        Celery bridge threads
                |
                v
        Celery trace function
                |
                v
        AsyncIO executor
                |
                v
        persistent asyncio event loop

    The bridge threads are necessary because Celery's tracing
    function is synchronous.

    The asyncio executor is where async task bodies actually run.
    """

    signal_safe = False
    is_green = False
    body_can_be_buffer = True

    def __init__(
            self,
            *args: Any,
            max_tasks: int | None = None,
            slow_task_threshold: float | None = None,
            thread_name: str = "celery-asyncio",
            **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        app = kwargs.get("app")
        self._apply_results = {}
        self._apply_results_lock = threading.Lock()
        if slow_task_threshold is None and app is not None:
            slow_task_threshold = app.conf.get(
                "ASYNC_CELERY_SLOW_TASK_THRESHOLD",
                30.0,
            )

        if slow_task_threshold is None:
            slow_task_threshold = 30.0

        self.slow_task_threshold = float(slow_task_threshold)

        configured_limit = (
            max_tasks
            if max_tasks is not None
            else self.limit
        )

        if configured_limit is None:
            configured_limit = 20

        self.max_tasks = int(configured_limit)

        if self.max_tasks < 1:
            raise ValueError(
                "AsyncIOPool max_tasks must be greater than zero"
            )

        if slow_task_threshold < 0:
            raise ValueError(
                "slow_task_threshold must be greater than or equal to zero"
            )

        # Persistent asyncio event loop.
        self.async_executor = AsyncExecutor(
            max_tasks=self.max_tasks,
            thread_name=thread_name,
            slow_task_threshold=self.slow_task_threshold,
        )

        self.asyncio_hub_wakeup = None
        # Celery tracing must NOT run on the asyncio event-loop
        # thread. These threads execute Celery's synchronous trace
        # function.
        self.executor = ThreadPoolExecutor(
            max_workers=self.max_tasks,
            thread_name_prefix="celery-async-bridge",
        )

        logger.info(
            "AsyncIOPool initialized: max_tasks=%s",
            self.max_tasks,
        )

    def _wake_celery_hub(self) -> None:
        wakeup = self.asyncio_hub_wakeup

        if wakeup is None:
            return

        try:
            wakeup.wake()
        except Exception:
            logger.debug(
                "Failed to wake Celery Hub",
                exc_info=True,
            )

    def _get_info(self) -> dict[str, Any]:
        info = super()._get_info()

        return {
            **info,
            "max-concurrency": self.max_tasks,
            "asyncio-running": self.async_executor.running,
            "asyncio-available": self.async_executor.available,
            "bridge-threads": len(self.executor._threads),
        }

    @property
    def num_processes(self) -> int:
        return self.max_tasks

    def on_start(self) -> None:
        logger.info(
            "Starting AsyncIOPool: max_tasks=%s",
            self.max_tasks,
        )

        self.async_executor.start()

        super().on_start()

    def on_stop(self) -> None:
        logger.info("Stopping AsyncIOPool")

        # Stop asyncio first so bridge threads blocked in
        # future.result() can be released.
        self.async_executor.shutdown(
            wait=True,
            timeout=10,
        )

        # Now the bridge threads can finish.
        self.executor.shutdown(
            wait=True,
            cancel_futures=False,
        )

        super().on_stop()

    def on_terminate(self) -> None:

        self.async_executor.shutdown(
            wait=False,
        )

        self.executor.shutdown(
            wait=False,
            cancel_futures=True,
        )

        super().on_terminate()

    def terminate_job(self, pid, signal=None):
        """Celery compatibility hook.

        Async tasks are cancelled through ApplyResult.terminate()
        rather than by terminating the worker process.
        """
        return None

    def on_apply(
            self,
            target,
            args=None,
            kwargs=None,
            callback=None,
            accept_callback=None,
            **options,
    ):
        args = args or ()
        kwargs = kwargs or {}

        if not self.async_executor.is_running:
            raise RuntimeError("AsyncIO executor is not running")

        task_id = options.get("correlation_id")

        future = self.executor.submit(
            self._run_celery_target,
            target,
            args,
            kwargs,
            callback,
            accept_callback,
            task_id,
        )

        def terminate(signal=None):
            if task_id is not None:
                if self.async_executor.cancel_task(
                        task_id,
                        reason=signal,
                ):
                    return True

            return future.cancel()

        result = ApplyResult(
            future,
            terminate_callback=terminate,
        )

        if task_id is not None:
            with self._apply_results_lock:
                self._apply_results[task_id] = result

            def cleanup(_future):
                with self._apply_results_lock:
                    self._apply_results.pop(task_id, None)

            future.add_done_callback(cleanup)

        return result

    def _run_celery_target(
            self,
            target: Callable[..., Any],
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
            callback: Callable[..., Any] | None,
            accept_callback: Callable[..., Any] | None,
            task_id=None
    ) -> Any:
        token = set_async_executor(self.async_executor)
        started = time.monotonic()

        try:
            if accept_callback:
                accept_callback(
                    os.getpid(),
                    time.monotonic(),
                )

            result = target(*args, **kwargs)

            if inspect.isawaitable(result):
                future = self.async_executor.submit(
                    result,
                    task_id=task_id,
                    task_name=getattr(target, "__qualname__", None),
                )

                try:
                    result = future.result()
                except CancelledError:
                    raise

            if callback:
                callback(result)

            return result


        except CancelledError:

            logger.info(

                "Celery task execution cancelled after %.3fs",

                time.monotonic() - started,

            )

            raise


        except BaseException:

            logger.exception(

                "Celery task execution failed after %.3fs",

                time.monotonic() - started,

            )

            raise

        finally:
            reset_async_executor(token)
            self._wake_celery_hub()
