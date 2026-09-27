import pytest
from flask import Flask

from flask_async_celery import AsyncCelery


def test_extension_initializes():
    app = Flask(__name__)

    celery = AsyncCelery(
        app,
        broker_url="redis://127.0.0.1:6379/0",
        result_backend="redis://127.0.0.1:6379/1",
        max_tasks=5,
        disable_prefetch=True,
    )

    assert app.extensions["async_celery"] is celery
    assert celery.max_tasks == 5
    assert celery.disable_prefetch is True
    assert celery.celery.conf.worker_concurrency == 5
    assert celery.celery.conf.worker_disable_prefetch is True
    assert (
        celery.celery.conf.worker_pool
        == "flask_async_celery.pool:AsyncIOPool"
    )


def test_extension_rejects_invalid_concurrency():
    app = Flask(__name__)

    with pytest.raises(ValueError):
        AsyncCelery(
            app,
            max_tasks=0,
        )