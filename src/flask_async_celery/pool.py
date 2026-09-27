from __future__ import annotations
import inspect
import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any, Callable
import time
from celery.concurrency.base import BasePool, apply_target

from .executor import AsyncExecutor
from .task import reset_async_executor, set_async_executor

logger = logging.getLogger(__name__)


class ApplyResult:
    """
    Celery-compatible result wrapper around a Future.
    """

    def __init__(self, future: Future) -> None:
        self.f = future
        self.get = future.result

    def wait(self, timeout: float | None = None) -> None:
        wait([self.f], timeout)

    def ready(self) -> bool:
        return self.f.done()

    def successful(self) -> bool:
        if not self.f.done():
            return False

        return self.f.exception() is None


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
        thread_name: str = "celery-asyncio",
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

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

        # Persistent asyncio event loop.
        self.async_executor = AsyncExecutor(
            max_tasks=self.max_tasks,
            thread_name=thread_name,
        )

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

        self.executor.shutdown(
            wait=True,
            cancel_futures=False,
        )

        self.async_executor.shutdown(
            wait=True,
            timeout=10,
        )

        super().on_stop()

    def on_terminate(self) -> None:
        logger.warning("Terminating AsyncIOPool")

        self.executor.shutdown(
            wait=False,
            cancel_futures=True,
        )

        self.async_executor.shutdown(
            wait=False,
        )

        super().on_terminate()

    def on_apply(
        self,
        target: Callable[..., Any],
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        callback: Callable[..., Any] | None = None,
        accept_callback: Callable[..., Any] | None = None,
        **options: Any,
    ) -> ApplyResult:
        """
        Submit Celery's tracing function to a bridge thread.

        The bridge thread executes Celery's normal synchronous
        tracing machinery.

        AsyncTask then detects the coroutine and submits it to
        the persistent asyncio event loop.
        """

        args = args or ()
        kwargs = kwargs or {}

        if not self.async_executor.is_running:
            raise RuntimeError(
                "AsyncIO executor is not running"
            )

        future = self.executor.submit(
            self._run_celery_target,
            target,
            args,
            kwargs,
            callback,
            accept_callback,
        )

        return ApplyResult(future)

    def _run_celery_target(
            self,
            target: Callable[..., Any],
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
            callback: Callable[..., Any] | None,
            accept_callback: Callable[..., Any] | None,
    ) -> Any:
        token = set_async_executor(self.async_executor)

        try:
            if accept_callback:
                accept_callback(os.getpid(), time.monotonic())

            result = target(*args, **kwargs)

            # Async Celery target:
            # run the coroutine on the persistent asyncio event loop.
            if inspect.isawaitable(result):
                future = self.async_executor.submit(result)
                result = future.result()

            if callback:
                callback(result)

            return result

        except BaseException:
            raise

        finally:
            reset_async_executor(token)

    def _get_info(self) -> dict[str, Any]:
        info = super()._get_info()

        info.update(
            {
                "max-concurrency": self.max_tasks,
                "asyncio-running": self.async_executor.running,
                "asyncio-available": self.async_executor.available,
                "bridge-threads": len(self.executor._threads),
            }
        )

        return info