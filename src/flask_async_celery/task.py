from __future__ import annotations

import inspect
from contextvars import ContextVar
from typing import Any
from concurrent.futures import CancelledError
from celery import Task
from celery.exceptions import Ignore
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
    """
    Celery Task base for native async tasks.

    Async tasks are executed on the persistent asyncio event loop
    managed by AsyncExecutor.

    When registered through AsyncCelery, the Flask application
    is made available through the Celery application and an
    application context is pushed while the async task executes.
    """

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

        # The Flask application is attached to the Celery app by AsyncCelery.
        flask_app = getattr(self._get_app(), "flask_app", None)

        async def execute_with_request() -> Any:
            # Restore the Celery request inside the asyncio thread.
            self.push_request(**request.__dict__)

            try:
                if flask_app is not None:
                    with flask_app.app_context():
                        return await self.run(*args, **kwargs)

                return await self.run(*args, **kwargs)

            finally:
                self.pop_request()

        coroutine = execute_with_request()

        try:
            future = executor.submit(
                coroutine,
                task_id=self.request.id,
                task_name=self.name,
            )
        except BaseException:
            if inspect.iscoroutine(coroutine):
                coroutine.close()
            raise

        try:
            return future.result()
        except CancelledError as exc:
            if executor.get_cancel_reason(request.id) is not None:
                raise Ignore("Task cancelled") from exc
            raise
        finally:
            executor.clear_cancel_reason(request.id)
