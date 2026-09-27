import asyncio

from celery.contrib.testing.worker import start_worker
from flask import Flask

from flask_async_celery import AsyncCelery, AsyncTask


def test_flask_async_celery_end_to_end():
    app = Flask(__name__)

    celery = AsyncCelery(
        app,
        broker_url="redis://127.0.0.1:6379/0",
        result_backend="redis://127.0.0.1:6379/1",
        max_tasks=2,
        disable_prefetch=True,
    )

    @celery.task(
        base=AsyncTask,
        name="test.extension_end_to_end",
    )
    async def extension_task(value):
        await asyncio.sleep(0.2)
        return value * 2

    with start_worker(
        celery.celery,
        concurrency=2,
        pool="flask_async_celery.pool:AsyncIOPool",
        loglevel="INFO",
        perform_ping_check=False,
    ):
        results = [
            extension_task.delay(1),
            extension_task.delay(2),
            extension_task.delay(3),
        ]

        values = [
            result.get(timeout=10)
            for result in results
        ]

    assert values == [2, 4, 6]