from __future__ import annotations

import threading
import time

import pytest
from celery.contrib.testing.worker import start_worker

from flask_async_celery.task import AsyncTask


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

        attempts["count"] += 1

        if attempts["count"] == 1:

            try:
                result = self.retry(countdown=1)

            except BaseException as exc:

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


def test_async_task_exception_does_not_break_worker(celery_app):
    """
    Verify that an exception from an async Celery task is reported as
    a normal Celery task failure and that the same worker can execute
    another async task afterward.
    """

    @celery_app.task(
        name="test.async_failure",
        base=AsyncTask,
    )
    async def failing_task():
        raise ValueError("intentional async failure")

    @celery_app.task(
        name="test.async_success_after_failure",
        base=AsyncTask,
    )
    async def successful_task():
        return "success"

    with start_worker(
            celery_app,
            concurrency=5,
            pool="flask_async_celery.pool:AsyncIOPool",
            loglevel="INFO",
            perform_ping_check=False,
    ):
        failed_result = failing_task.delay()

        with pytest.raises(
                ValueError,
                match="intentional async failure",
        ):
            failed_result.get(timeout=10)

        successful_result = successful_task.delay()

        assert successful_result.get(timeout=10) == "success"


def test_real_celery_worker_handles_large_async_workload(
        celery_app,
):
    """
    Verify that the real Celery worker can process a large async workload
    with Redis worker_disable_prefetch enabled.

    This test intentionally uses the production AsyncIOPool and its real
    Celery/Kombu Hub wakeup integration.

    The test verifies:

    - real Celery worker
    - real Kombu Hub
    - Redis broker
    - worker_disable_prefetch=True
    - bounded async concurrency
    - all tasks complete
    - no task is lost or duplicated
    - the asyncio executor never exceeds max_tasks
    - the pool has a real Hub wakeup attached
    """

    import asyncio
    import time

    from celery.contrib.testing.worker import start_worker
    from flask_async_celery.task import AsyncTask

    max_tasks = 5
    total_tasks = 200

    # ------------------------------------------------------------------
    # Celery configuration
    # ------------------------------------------------------------------

    celery_app.conf.update(
        worker_disable_prefetch=True,
        worker_pool_putlocks=False,
    )

    # ------------------------------------------------------------------
    # Concurrency tracking
    # ------------------------------------------------------------------

    state = {
        "running": 0,
        "maximum": 0,
    }

    @celery_app.task(
        name="test.large_async_workload",
        base=AsyncTask,
    )
    async def workload(number):
        state["running"] += 1
        state["maximum"] = max(
            state["maximum"],
            state["running"],
        )

        try:
            await asyncio.sleep(0.02)
            return number
        finally:
            state["running"] -= 1

    # ------------------------------------------------------------------
    # Start real Celery worker
    # ------------------------------------------------------------------

    started = time.monotonic()

    with start_worker(
            celery_app,
            concurrency=max_tasks,
            pool="flask_async_celery.pool:AsyncIOPool",
            loglevel="INFO",
            perform_ping_check=False,
    ) as worker:

        consumer = worker.consumer
        hub = consumer.hub

        assert hub is not None, (
            "Celery consumer Hub was not created"
        )

        # --------------------------------------------------------------
        # Verify the real AsyncIOPool is running.
        # --------------------------------------------------------------

        pool = worker.pool

        assert pool.async_executor.is_running

        assert pool.max_tasks == max_tasks

        # --------------------------------------------------------------
        # Verify production Hub wakeup integration.
        #
        # The bootstep should have created and attached the actual
        # HubWakeup object. We do NOT create a test replacement here.
        # --------------------------------------------------------------

        hub_wakeup = getattr(
            pool,
            "asyncio_hub_wakeup",
            None,
        )

        assert hub_wakeup is not None, (
            "AsyncIOPool has no HubWakeup attached"
        )

        assert hub_wakeup.hub is hub

        # The wakeup pipe must actually be registered with the real Hub.
        assert hub_wakeup.read_fd in hub.readers

        # --------------------------------------------------------------
        # Verify Redis QoS configuration.
        # --------------------------------------------------------------

        connection = consumer.connection
        channel = connection.default_channel
        qos = channel.qos

        assert qos.prefetch_count == max_tasks

        # --------------------------------------------------------------
        # Submit workload.
        # --------------------------------------------------------------

        workload_started = time.monotonic()

        results = [
            workload.delay(number)
            for number in range(total_tasks)
        ]

        submitted_elapsed = (
                time.monotonic() - workload_started
        )

        # --------------------------------------------------------------
        # Wait for all results.
        # --------------------------------------------------------------

        values = [
            result.get(timeout=30)
            for result in results
        ]

        elapsed = (
                time.monotonic()
                - workload_started
        )

        # --------------------------------------------------------------
        # Assertions
        # --------------------------------------------------------------

        assert values == list(range(total_tasks))

        assert state["running"] == 0

        assert state["maximum"] <= max_tasks

        # The executor should have enforced the configured limit.
        assert pool.async_executor.running == 0

        assert pool.async_executor.available == max_tasks

        # Production wakeup must still be attached.
        assert pool.asyncio_hub_wakeup is hub_wakeup

        # We should have processed the workload concurrently.
        #
        # 200 * 0.02 = 4 seconds of work if executed sequentially.
        # With five concurrent tasks the actual async work is roughly
        # 0.8 seconds, plus Celery/Redis overhead.
        assert elapsed < 10


def test_real_celery_hub_wakeup_survives_reset(celery_app):
    worker_context = start_worker(
        celery_app,
        perform_ping_check=False,
        concurrency=5,
        pool="flask_async_celery.pool:AsyncIOPool",
        loglevel="INFO",
    )

    worker = worker_context.__enter__()

    try:
        pool = worker.pool
        hub = worker.hub
        wakeup = pool.asyncio_hub_wakeup

        assert wakeup is not None
        assert wakeup.hub is hub
        assert wakeup.read_fd in hub.readers

        # Phase 1
        results = [
            celery_app.send_task(
                "test.hub_wakeup_reset",
                args=(i,),
            )
            for i in range(50)
        ]

        for result in results:
            assert result.get(timeout=5) is not None

        # Reset the Hub on the Hub's own thread.
        reset_done = threading.Event()
        reset_error = []

        def reset_hub():
            try:
                hub.reset()
            except BaseException as exc:
                reset_error.append(exc)
            finally:
                reset_done.set()

        hub.call_soon(reset_hub)

        assert reset_done.wait(timeout=5), (
            "Hub reset did not execute"
        )

        if reset_error:
            raise reset_error[0]

        # Production wakeup should have re-registered.
        assert wakeup.read_fd in hub.readers

        # Phase 2
        started = time.monotonic()

        results = [
            celery_app.send_task(
                "test.hub_wakeup_reset",
                args=(i,),
            )
            for i in range(50)
        ]

        for result in results:
            assert result.get(timeout=5) is not None

        elapsed = time.monotonic() - started

        assert elapsed < 5.0

    finally:
        worker_context.__exit__(None, None, None)
