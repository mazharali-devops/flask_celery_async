import pytest
from celery import Celery
from celery.exceptions import Retry
import pytest
from celery import Celery
import asyncio
from flask_async_celery.task import AsyncTask

@pytest.fixture
def celery_app():
    app = Celery(
        "async_worker_test",
        broker="redis://127.0.0.1:6379/0",
        backend="redis://127.0.0.1:6379/1",
    )

    app.conf.update(
        task_always_eager=False,
        task_track_started=True,
        worker_pool="flask_async_celery.pool:AsyncIOPool",
        worker_concurrency=5,
        worker_prefetch_multiplier=1,
    )

    @app.task(
        name="test.async_sleep",
        base=AsyncTask,
    )
    async def async_sleep(number: int) -> int:
        await asyncio.sleep(2)
        return number

    # IMPORTANT: this must be inside the fixture
    return app