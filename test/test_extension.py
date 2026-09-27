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


def test_flask_config_overrides_defaults():
    app = Flask(__name__)

    app.config["ASYNC_CELERY_MAX_TASKS"] = 7
    app.config["ASYNC_CELERY_DISABLE_PREFETCH"] = False

    celery = AsyncCelery(
        app,
        broker_url="redis://127.0.0.1:6379/0",
        result_backend="redis://127.0.0.1:6379/1",
    )

    assert celery.max_tasks == 7
    assert celery.disable_prefetch is False

    assert celery.celery.conf.worker_concurrency == 7
    assert celery.celery.conf.worker_disable_prefetch is not True


def test_prefetch_can_be_disabled():
    app = Flask(__name__)

    celery = AsyncCelery(
        app,
        max_tasks=3,
        disable_prefetch=False,
    )

    assert celery.max_tasks == 3
    assert celery.disable_prefetch is False