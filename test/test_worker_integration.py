from __future__ import annotations

import time
from flask_async_celery.task import AsyncTask

from celery.contrib.testing.worker import start_worker




def test_real_celery_worker_runs_async_tasks_concurrently(celery_app):
    """
    Start a real Celery worker using AsyncIOPool and verify that
    multiple async tasks execute concurrently.
    """

    with start_worker(
        celery_app,
        concurrency=5,
        pool="flask_async_celery.pool:AsyncIOPool",
        loglevel="INFO",
        perform_ping_check=False,
    ):
        started = time.monotonic()

        results = [
            celery_app.tasks["test.async_sleep"].delay(number)
            for number in range(5)
        ]

        values = [
            result.get(timeout=10)
            for result in results
        ]

        elapsed = time.monotonic() - started

    assert values == [0, 1, 2, 3, 4]

    # Five 2-second async tasks should execute concurrently.
    #
    # Allow extra time for Redis communication and worker startup,
    # but it should still be substantially below sequential execution.
    assert elapsed < 6


def test_async_task_retry(celery_app):
    attempts = {"count": 0}

    @celery_app.task(
        bind=True,
        base=AsyncTask,
        max_retries=1,
        default_retry_delay=1,
    )
    async def retry_task(self):
        import threading

        attempts["count"] += 1

        print(
            "\n=== ASYNC TASK ===",
            "\nthread:", threading.current_thread().name,
            "\nattempt:", attempts["count"],
            "\ntask id:", self.request.id,
            "\nretries:", self.request.retries,
            "\ncalled_directly:", self.request.called_directly,
            "\nis_eager:", self.request.is_eager,
            "\ndelivery_info:", self.request.delivery_info,
            "\nexecution_options:", self.request.as_execution_options(),
        )

        if attempts["count"] == 1:
            print("\n=== CALLING RETRY ===")

            try:
                result = self.retry(countdown=1)
                print("RETRY RETURNED:", result)

            except BaseException as exc:
                print(
                    "\n=== RETRY RAISED ===",
                    "\nexception:", repr(exc),
                    "\ntype:", type(exc),
                    "\nthread:", threading.current_thread().name,
                )
                raise

        return "success"

    with start_worker(
        celery_app,
        concurrency=5,
        pool="flask_async_celery.pool:AsyncIOPool",
        loglevel="DEBUG",
        perform_ping_check=False,
    ):
        result = retry_task.delay()

        assert result.get(timeout=15) == "success"

    assert attempts["count"] == 2


def test_sync_task_retry(celery_app):
    attempts = {"count": 0}

    @celery_app.task(
        name="test.sync_retry",
        bind=True,
        max_retries=1,
        default_retry_delay=1,
    )
    def retry_task(self):
        attempts["count"] += 1

        if attempts["count"] == 1:
            raise self.retry(countdown=1)

        return "success"

    with start_worker(
        celery_app,
        concurrency=5,
        pool="flask_async_celery.pool:AsyncIOPool",
        loglevel="INFO",
        perform_ping_check=False,
    ):
        result = retry_task.delay()

        assert result.get(timeout=15) == "success"

    assert attempts["count"] == 2

