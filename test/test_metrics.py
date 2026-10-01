from unittest.mock import MagicMock

from prometheus_client import generate_latest

from flask_async_celery.metrics import (
    AsyncCeleryCollector,
    create_registry,
)
from flask import Flask

from flask_async_celery import AsyncCelery


def make_celery_app(replies):
    celery_app = MagicMock()
    celery_app.control.broadcast.return_value = replies
    return celery_app


def test_collector_exports_worker_metrics():
    replies = [
        {
            "celery@worker1": {
                "running": 2,
                "available": 3,
                "max_tasks": 5,
                "completed": 10,
                "failed": 2,
                "cancelled": 1,
                "average_duration": 1.5,
                "slow_task_threshold": 30.0,
                "long_running": 1,
                "event_loop_running": True,
                "stopping": False,
            }
        }
    ]

    app = make_celery_app(replies)
    registry = create_registry(app)

    output = generate_latest(registry).decode()

    assert 'flask_async_celery_running_tasks{worker="celery@worker1"} 2.0' in output
    assert 'flask_async_celery_available_slots{worker="celery@worker1"} 3.0' in output
    assert 'flask_async_celery_max_tasks{worker="celery@worker1"} 5.0' in output

    assert 'flask_async_celery_completed_total{worker="celery@worker1"} 10.0' in output
    assert 'flask_async_celery_failed_total{worker="celery@worker1"} 2.0' in output
    assert 'flask_async_celery_cancelled_total{worker="celery@worker1"} 1.0' in output

    assert (
        'flask_async_celery_average_duration_seconds'
        '{worker="celery@worker1"} 1.5'
    ) in output

    assert (
        'flask_async_celery_slow_task_threshold_seconds'
        '{worker="celery@worker1"} 30.0'
    ) in output

    assert (
        'flask_async_celery_long_running_tasks'
        '{worker="celery@worker1"} 1.0'
    ) in output

    assert (
        'flask_async_celery_event_loop_running'
        '{worker="celery@worker1"} 1.0'
    ) in output

    assert (
        'flask_async_celery_stopping'
        '{worker="celery@worker1"} 0.0'
    ) in output


def test_collector_supports_multiple_workers():
    replies = [
        {
            "celery@worker1": {
                "running": 2,
                "available": 3,
                "max_tasks": 5,
                "completed": 10,
                "failed": 1,
                "cancelled": 0,
                "average_duration": 1.0,
                "slow_task_threshold": 30.0,
                "long_running": 0,
                "event_loop_running": True,
                "stopping": False,
            }
        },
        {
            "celery@worker2": {
                "running": 4,
                "available": 1,
                "max_tasks": 5,
                "completed": 20,
                "failed": 3,
                "cancelled": 2,
                "average_duration": 2.0,
                "slow_task_threshold": 60.0,
                "long_running": 2,
                "event_loop_running": True,
                "stopping": False,
            }
        },
    ]

    app = make_celery_app(replies)
    registry = create_registry(app)

    output = generate_latest(registry).decode()

    assert (
        'flask_async_celery_running_tasks'
        '{worker="celery@worker1"} 2.0'
    ) in output

    assert (
        'flask_async_celery_running_tasks'
        '{worker="celery@worker2"} 4.0'
    ) in output

    assert (
        'flask_async_celery_completed_total'
        '{worker="celery@worker1"} 10.0'
    ) in output

    assert (
        'flask_async_celery_completed_total'
        '{worker="celery@worker2"} 20.0'
    ) in output


def test_collector_handles_no_workers():
    app = make_celery_app([])

    registry = create_registry(app)

    output = generate_latest(registry).decode()

    assert output == ""


def test_collector_broadcasts_async_stats():
    app = make_celery_app([])

    registry = create_registry(app)

    generate_latest(registry)

    app.control.broadcast.assert_called_once_with(
        "async_stats",
        reply=True,
    )


def test_collector_can_be_used_directly():
    replies = [
        {
            "celery@worker1": {
                "running": 1,
                "available": 4,
                "max_tasks": 5,
                "completed": 5,
                "failed": 0,
                "cancelled": 0,
                "average_duration": 0.5,
                "slow_task_threshold": 30.0,
                "long_running": 0,
                "event_loop_running": True,
                "stopping": False,
            }
        }
    ]

    app = make_celery_app(replies)
    collector = AsyncCeleryCollector(app)

    metrics = list(collector.collect())

    assert len(metrics) == 11




def test_metrics_disabled_by_default():
    app = Flask(__name__)

    async_celery = AsyncCelery(app)

    client = app.test_client()

    response = client.get("/metrics")

    assert response.status_code == 404


def test_metrics_enabled():
    app = Flask(__name__)

    app.config["PROMETHEUS_ENABLED"] = True

    async_celery = AsyncCelery(app)

    async_celery.celery.control.broadcast = MagicMock(
        return_value=[
            {
                "celery@worker1": {
                    "running": 2,
                    "available": 3,
                    "max_tasks": 5,
                    "completed": 10,
                    "failed": 1,
                    "cancelled": 0,
                    "average_duration": 1.5,
                    "slow_task_threshold": 30.0,
                    "long_running": 1,
                    "event_loop_running": True,
                    "stopping": False,
                }
            }
        ]
    )

    client = app.test_client()

    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.content_type.startswith("text/plain")

    body = response.data.decode()

    assert "flask_async_celery_running_tasks" in body
    assert 'worker="celery@worker1"' in body
    assert "2.0" in body
