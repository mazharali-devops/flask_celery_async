from unittest.mock import MagicMock

from flask import Flask

from flask_async_celery.extension import AsyncCelery


def test_metrics_endpoint_is_disabled_by_default():
    app = Flask(__name__)

    celery_app = MagicMock()

    extension = AsyncCelery.__new__(AsyncCelery)
    extension.app = app
    extension.celery = celery_app

    extension._register_metrics_endpoint()

    client = app.test_client()

    response = client.get("/metrics")

    assert response.status_code == 404


def test_metrics_endpoint_is_registered_when_enabled():
    app = Flask(__name__)
    app.config["PROMETHEUS_ENABLED"] = True

    celery_app = MagicMock()

    celery_app.control.broadcast.return_value = [
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

    extension = AsyncCelery.__new__(AsyncCelery)
    extension.app = app
    extension.celery = celery_app

    extension._register_metrics_endpoint()

    client = app.test_client()

    response = client.get("/metrics")

    assert response.status_code == 200

    body = response.data.decode()

    assert "flask_async_celery_running_tasks" in body
    assert 'worker="celery@worker1"' in body
    assert "2.0" in body

    celery_app.control.broadcast.assert_called_once_with(
        "async_stats",
        reply=True,
    )
