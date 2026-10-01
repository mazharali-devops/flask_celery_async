from __future__ import annotations

from typing import Any

from celery import Celery
from flask import Flask
from flask import Response
from .bootstep import AsyncIOBootStep
from .hub_bootstep import AsyncIOHubWakeupBootStep
from .task import AsyncTask


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
        slow_task_threshold: float = 30.0,
        **celery_options: Any,
    ) -> None:
        if max_tasks < 1:
            raise ValueError(
                "max_tasks must be greater than zero"
            )

        self.app: Flask | None = None
        self.celery: Celery

        self.max_tasks = max_tasks
        self.disable_prefetch = disable_prefetch
        self.slow_task_threshold = slow_task_threshold
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

        # Make the Flask application available to AsyncTask
        # when the task executes in the Celery worker.
        self.celery.flask_app = app

        app.config.setdefault(
            "ASYNC_CELERY_MAX_TASKS",
            self.max_tasks,
        )

        app.config.setdefault(
            "ASYNC_CELERY_DISABLE_PREFETCH",
            self.disable_prefetch,
        )

        app.config.setdefault(
            "ASYNC_CELERY_SLOW_TASK_THRESHOLD",
            self.slow_task_threshold,
        )

        app.config.setdefault(
            "PROMETHEUS_ENABLED",
            False,
        )

        self.max_tasks = int(
            app.config["ASYNC_CELERY_MAX_TASKS"]
        )

        self.disable_prefetch = bool(
            app.config["ASYNC_CELERY_DISABLE_PREFETCH"]
        )

        self.slow_task_threshold = float(
            app.config["ASYNC_CELERY_SLOW_TASK_THRESHOLD"]
        )

        self.celery.conf["ASYNC_CELERY_SLOW_TASK_THRESHOLD"] = (
            self.slow_task_threshold
        )
        if self.max_tasks < 1:
            raise ValueError(
                "ASYNC_CELERY_MAX_TASKS must be greater than zero"
            )

        self._configure()
        self._register_metrics_endpoint()

    def _configure(self) -> None:
        self.celery.conf.update(
            worker_pool="flask_async_celery.pool:AsyncIOPool",
            worker_concurrency=self.max_tasks,
            worker_disable_prefetch=self.disable_prefetch,
        )

        self.celery.steps["worker"].add(
            AsyncIOBootStep
        )

        self.celery.steps["worker"].add(
            AsyncIOHubWakeupBootStep
        )

    def task(self, *args: Any, **kwargs: Any):
        """
        Register a Celery task.

        Async tasks use AsyncTask by default.

        Usage:

            @async_celery.task
            async def task():
                ...

        A custom task base can still be provided:

            @async_celery.task(base=CustomTask)
            def task():
                ...
        """

        kwargs.setdefault("base", AsyncTask)

        return self.celery.task(*args, **kwargs)

    def send_task(self, *args: Any, **kwargs: Any):
        return self.celery.send_task(*args, **kwargs)

    def _register_metrics_endpoint(self) -> None:
        """Register the Prometheus metrics endpoint."""

        app = self.app

        if app is None:
            return

        if not app.config.get("PROMETHEUS_ENABLED", False):
            return

        try:
            from .metrics import create_registry
            from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
        except ImportError as exc:
            raise RuntimeError(
                "Prometheus support requires prometheus-client. "
                "Install it with: pip install flask-async-celery[prometheus]"
            ) from exc

        registry = create_registry(self.celery)

        @app.route("/metrics")
        def metrics():
            return Response(
                generate_latest(registry),
                mimetype=CONTENT_TYPE_LATEST,
            )

    def __getattr__(self, name: str) -> Any:
        return getattr(self.celery, name)
