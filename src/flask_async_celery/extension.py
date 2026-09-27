from __future__ import annotations

from typing import Any

from celery import Celery
from flask import Flask


class AsyncCelery:
    """
    Flask integration for flask-async-celery.

    Configures a Celery application to use AsyncIOPool and
    optionally enables Celery consumer-side backpressure.
    """

    def __init__(
        self,
        app: Flask | None = None,
        *,
        celery: Celery | None = None,
        broker_url: str | None = None,
        result_backend: str | None = None,
        max_tasks: int = 20,
        disable_prefetch: bool = True,
        **celery_options: Any,
    ) -> None:
        if max_tasks < 1:
            raise ValueError("max_tasks must be greater than zero")

        self.app: Flask | None = None
        self.celery: Celery

        self.max_tasks = max_tasks
        self.disable_prefetch = disable_prefetch

        if celery is not None:
            self.celery = celery
        else:
            self.celery = Celery(
                app.import_name if app is not None else "flask_async_celery",
                broker=broker_url,
                backend=result_backend,
                **celery_options,
            )

        self._configure()

        if app is not None:
            self.init_app(app)

    def init_app(self, app: Flask) -> None:
        self.app = app

        app.extensions["async_celery"] = self

        app.config.setdefault(
            "ASYNC_CELERY_MAX_TASKS",
            self.max_tasks,
        )

        app.config.setdefault(
            "ASYNC_CELERY_DISABLE_PREFETCH",
            self.disable_prefetch,
        )

        self.max_tasks = int(
            app.config["ASYNC_CELERY_MAX_TASKS"]
        )

        self.disable_prefetch = bool(
            app.config["ASYNC_CELERY_DISABLE_PREFETCH"]
        )

        if self.max_tasks < 1:
            raise ValueError(
                "ASYNC_CELERY_MAX_TASKS must be greater than zero"
            )

        self._configure()

    def _configure(self) -> None:
        self.celery.conf.update(
            worker_pool="flask_async_celery.pool:AsyncIOPool",
            worker_concurrency=self.max_tasks,
            worker_disable_prefetch=self.disable_prefetch,
        )

    def task(self, *args: Any, **kwargs: Any):
        """
        Register a Celery task.

        Usage:

            @celery.task
            async def task():
                ...

        or:

            @celery.task(base=AsyncTask)
            async def task():
                ...
        """
        return self.celery.task(*args, **kwargs)

    def send_task(self, *args: Any, **kwargs: Any):
        return self.celery.send_task(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.celery, name)