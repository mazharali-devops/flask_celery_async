from __future__ import annotations

import asyncio
import time

from celery.contrib.testing.worker import start_worker

from flask_async_celery import AsyncTask


def test_real_worker_hub_wakeup():
    from flask import Flask
    from flask_async_celery import AsyncCelery

    app = Flask(__name__)

    celery = AsyncCelery(
        app,
        broker_url="redis://127.0.0.1:6379/0",
        result_backend="redis://127.0.0.1:6379/1",
        max_tasks=5,
        disable_prefetch=False,
    )

    @celery.task(
        name="test.hub_wakeup",
        base=AsyncTask,
    )
    async def workload(number):
        await asyncio.sleep(0.02)
        return number

    with start_worker(
            celery.celery,
            concurrency=5,
            pool="flask_async_celery.pool:AsyncIOPool",
            loglevel="INFO",
            perform_ping_check=False,
            pool_putlocks=False,
    ) as worker:
        print("\n>>> WORKER HUB:", worker.hub)
        print(">>> HUB WAKEUP:", getattr(worker, "asyncio_hub_wakeup", None))

        wakeup = getattr(worker, "asyncio_hub_wakeup", None)

        assert wakeup is not None
        assert wakeup._registered is True

        started = time.monotonic()

        results = [
            workload.delay(i)
            for i in range(20)
        ]

        values = [
            result.get(timeout=10)
            for result in results
        ]

        elapsed = time.monotonic() - started

        print(f">>> RESULTS COMPLETE {elapsed:.2f}s")

        assert values == list(range(20))
        assert elapsed < 5.0

        print(">>> WAKEUP REGISTERED:", wakeup._registered)
        print(">>> WAKEUP CLOSED:", wakeup._closed)
