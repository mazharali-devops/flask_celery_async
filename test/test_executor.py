import asyncio
import time

from flask_async_celery.executor import (
    AsyncExecutor,
)


async def slow_task(number):

    await asyncio.sleep(2)

    return number


def test_tasks_run_concurrently():

    executor = AsyncExecutor(
        max_tasks=5
    )

    executor.start()

    try:

        started = time.monotonic()

        futures = [
            executor.submit(
                slow_task(i)
            )
            for i in range(5)
        ]

        results = [
            future.result(timeout=5)
            for future in futures
        ]

        elapsed = (
            time.monotonic() - started
        )

        assert results == [
            0, 1, 2, 3, 4
        ]

        # Five 2-second tasks should finish
        # in roughly 2 seconds, not 10.
        assert elapsed < 4

    finally:

        executor.shutdown()