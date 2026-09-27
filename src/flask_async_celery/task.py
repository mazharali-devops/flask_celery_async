from __future__ import annotations

import inspect
from contextvars import ContextVar
from typing import Any

from celery import Task


_async_executor: ContextVar[Any | None] = ContextVar(
    "flask_async_celery_executor",
    default=None,
)


def set_async_executor(executor: Any):
    return _async_executor.set(executor)


def reset_async_executor(token) -> None:
    _async_executor.reset(token)


def get_async_executor() -> Any | None:
    return _async_executor.get()


class AsyncTask(Task):
    abstract = True

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if not inspect.iscoroutinefunction(self.run):
            return super().__call__(*args, **kwargs)

        executor = get_async_executor()
        if executor is None:
            raise RuntimeError(
                "AsyncTask requires an AsyncExecutor. "
                "Make sure the task is running inside AsyncIOPool."
            )

        # Capture the real Celery request while we're still on
        # the Celery worker/bridge thread.
        request = self.request

        async def execute_with_request() -> Any:
            # Restore the Celery request inside the asyncio thread.
            self.push_request(**request.__dict__)

            try:
                return await self.run(*args, **kwargs)
            finally:
                self.pop_request()

        coroutine = execute_with_request()

        try:
            future = executor.submit(coroutine)
            return future.result()
        except BaseException:
            if inspect.iscoroutine(coroutine):
                coroutine.close()
            raise