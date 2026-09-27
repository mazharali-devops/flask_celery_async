import asyncio

from flask_async_celery.executor import AsyncExecutor


async def tracked_task(
    running: list[int],
    maximum: list[int],
):
    running[0] += 1
    maximum[0] = max(maximum[0], running[0])

    await asyncio.sleep(1)

    running[0] -= 1


def test_executor_respects_max_tasks():
    executor = AsyncExecutor(max_tasks=3)
    executor.start()

    try:
        running = [0]
        maximum = [0]

        futures = [
            executor.submit(
                tracked_task(running, maximum)
            )
            for _ in range(10)
        ]

        for future in futures:
            future.result(timeout=10)

        assert maximum[0] == 3

    finally:
        executor.shutdown()


import asyncio
import threading
import time

from celery.contrib.testing.worker import start_worker

from flask_async_celery.task import AsyncTask


def test_worker_disables_prefetch_for_async_pool(celery_app):
    """
    Verify that Celery's Redis worker_disable_prefetch mechanism
    limits task reservation to the AsyncIOPool concurrency.

    This tests consumer-side backpressure, not just executor
    concurrency.
    """

    celery_app.conf.worker_disable_prefetch = True

    running = 0
    max_running = 0
    lock = threading.Lock()

    @celery_app.task(
        name="test.backpressure_task",
        base=AsyncTask,
    )
    async def backpressure_task():
        nonlocal running, max_running

        with lock:
            running += 1
            max_running = max(max_running, running)

        try:
            await asyncio.sleep(1)
            return "done"
        finally:
            with lock:
                running -= 1

    with start_worker(
        celery_app,
        concurrency=2,
        pool="flask_async_celery.pool:AsyncIOPool",
        loglevel="INFO",
        perform_ping_check=False,
    ):
        results = [
            backpressure_task.delay()
            for _ in range(6)
        ]

        values = [
            result.get(timeout=15)
            for result in results
        ]

    assert values == ["done"] * 6
    assert max_running == 2