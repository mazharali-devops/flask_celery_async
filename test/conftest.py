import asyncio

import pytest
from flask import Flask

from flask_async_celery import AsyncCelery
from flask_async_celery.task import AsyncTask


@pytest.fixture
def celery_app():
    flask_app = Flask("async_worker_test")

    async_celery = AsyncCelery(
        flask_app,
        broker_url="redis://127.0.0.1:6379/0",
        result_backend="redis://127.0.0.1:6379/1",
        max_tasks=5,
        disable_prefetch=True,
    )

    app = async_celery.celery

    app.conf.update(
        task_always_eager=False,
        task_track_started=True,
        worker_prefetch_multiplier=1,

    )

    @app.task(name="test.async_sleep", base=AsyncTask)
    async def async_sleep(number: int) -> int:
        await asyncio.sleep(2)
        return number

    @app.task(name="test.hub_wakeup_reset", base=AsyncTask)
    async def hub_wakeup_reset(number: int) -> int:
        await asyncio.sleep(0.02)
        return number

    return app
