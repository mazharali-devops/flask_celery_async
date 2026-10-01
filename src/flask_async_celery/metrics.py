from __future__ import annotations

from prometheus_client import CollectorRegistry
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily


class AsyncCeleryCollector:
    """Collect AsyncIO Celery worker metrics."""

    def __init__(self, celery_app):
        self.celery_app = celery_app

    @staticmethod
    def _gauge(name, documentation, worker, value):
        metric = GaugeMetricFamily(
            name,
            documentation,
            labels=["worker"],
        )
        metric.add_metric([worker], value)
        return metric

    @staticmethod
    def _counter(name, documentation, worker, value):
        metric = CounterMetricFamily(
            name,
            documentation,
            labels=["worker"],
        )
        metric.add_metric([worker], value)
        return metric

    def collect(self):
        replies = self.celery_app.control.broadcast(
            "async_stats",
            reply=True,
        )

        for reply in replies:
            for worker, stats in reply.items():
                yield self._gauge(
                    "flask_async_celery_running_tasks",
                    "Number of currently running async tasks.",
                    worker,
                    stats.get("running", 0),
                )

                yield self._gauge(
                    "flask_async_celery_available_slots",
                    "Number of available AsyncIO execution slots.",
                    worker,
                    stats.get("available", 0),
                )

                yield self._gauge(
                    "flask_async_celery_max_tasks",
                    "Maximum number of concurrent async tasks.",
                    worker,
                    stats.get("max_tasks", 0),
                )

                yield self._counter(
                    "flask_async_celery_completed",
                    "Total number of completed async tasks.",
                    worker,
                    stats.get("completed", 0),
                )

                yield self._counter(
                    "flask_async_celery_failed",
                    "Total number of failed async tasks.",
                    worker,
                    stats.get("failed", 0),
                )

                yield self._counter(
                    "flask_async_celery_cancelled",
                    "Total number of cancelled async tasks.",
                    worker,
                    stats.get("cancelled", 0),
                )

                yield self._gauge(
                    "flask_async_celery_average_duration_seconds",
                    "Average duration of completed async tasks in seconds.",
                    worker,
                    stats.get("average_duration", 0.0),
                )

                yield self._gauge(
                    "flask_async_celery_long_running_tasks",
                    "Number of async tasks currently above the slow-task threshold.",
                    worker,
                    stats.get("long_running", 0),
                )

                yield self._gauge(
                    "flask_async_celery_slow_task_threshold_seconds",
                    "Configured slow-task threshold in seconds.",
                    worker,
                    stats.get("slow_task_threshold", 0.0),
                )

                yield self._gauge(
                    "flask_async_celery_event_loop_running",
                    "Whether the AsyncIO event loop is running.",
                    worker,
                    int(stats.get("event_loop_running", False)),
                )

                yield self._gauge(
                    "flask_async_celery_stopping",
                    "Whether the AsyncIO executor is stopping.",
                    worker,
                    int(stats.get("stopping", False)),
                )


def create_registry(celery_app) -> CollectorRegistry:
    """Create a Prometheus registry containing AsyncIO Celery metrics."""

    registry = CollectorRegistry()
    registry.register(AsyncCeleryCollector(celery_app))
    return registry
