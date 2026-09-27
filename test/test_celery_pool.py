from __future__ import annotations

import asyncio
import time

import pytest
from celery import Celery

from flask_async_celery.pool import AsyncIOPool


@pytest.fixture
def celery_app():
    app = Celery(
        "test_async_pool",
        broker="redis://127.0.0.1:6379/0",
        backend="redis://127.0.0.1:6379/1",
    )

    app.conf.update(
        task_always_eager=False,
        task_track_started=True,
        worker_pool="flask_async_celery.pool:AsyncIOPool",
        worker_concurrency=5,
    )

    return app


def test_asyncio_pool_executes_async_target(celery_app):
    """
    Verify that AsyncIOPool can execute an async Celery-style target
    concurrently.

    This test does not start a separate Celery worker process yet.
    It exercises the actual AsyncIOPool integration directly.
    """

    pool = AsyncIOPool(
        limit=5,
        app=celery_app,
    )

    pool.on_start()

    async def async_target(number):
        await asyncio.sleep(2)
        return number

    try:
        started = time.monotonic()

        futures = []

        for number in range(5):
            result = pool.on_apply(
                target=async_target,
                args=(number,),
                callback=lambda value: None,
            )

            futures.append(result)

        results = [
            future.get(timeout=5)
            for future in futures
        ]

        elapsed = time.monotonic() - started

        assert results == [0, 1, 2, 3, 4]

        # Five 2-second async tasks should run concurrently.
        assert elapsed < 4

    finally:
        pool.on_stop()