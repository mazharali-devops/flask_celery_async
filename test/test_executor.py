import asyncio
import time
from concurrent.futures import CancelledError

import pytest

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


@pytest.fixture
def executor():
    executor = AsyncExecutor(max_tasks=2)
    executor.start()

    yield executor

    executor.shutdown()


def test_exception_releases_execution_slot(executor):
    async def failing_task():
        await asyncio.sleep(0.05)
        raise RuntimeError("expected failure")

    future = executor.submit(failing_task())

    with pytest.raises(RuntimeError, match="expected failure"):
        future.result(timeout=5)

    assert executor.running == 0
    assert executor.available == executor.max_tasks


def test_multiple_exceptions_do_not_deadlock(executor):
    async def failing_task(number):
        await asyncio.sleep(0.02)
        raise RuntimeError(f"failure-{number}")

    futures = [
        executor.submit(failing_task(number))
        for number in range(10)
    ]

    for future in futures:
        with pytest.raises(RuntimeError):
            future.result(timeout=5)

    assert executor.running == 0
    assert executor.available == executor.max_tasks


def test_executor_recovers_after_exception(executor):
    async def failing_task():
        raise RuntimeError("boom")

    async def successful_task():
        await asyncio.sleep(0.05)
        return "success"

    failed = executor.submit(failing_task())

    with pytest.raises(RuntimeError, match="boom"):
        failed.result(timeout=5)

    assert executor.running == 0

    successful = executor.submit(successful_task())

    assert successful.result(timeout=5) == "success"

    assert executor.running == 0
    assert executor.available == executor.max_tasks


def test_concurrency_never_exceeds_max_tasks(executor):
    max_seen = 0
    lock = asyncio.Lock()

    async def tracked_task():
        nonlocal max_seen

        async with lock:
            max_seen = max(
                max_seen,
                executor.running,
            )

        await asyncio.sleep(0.1)

    futures = [
        executor.submit(tracked_task())
        for _ in range(10)
    ]

    for future in futures:
        future.result(timeout=5)

    assert max_seen <= executor.max_tasks
    assert executor.running == 0
    assert executor.available == executor.max_tasks


def test_executor_handles_success_and_failure_together(executor):
    async def successful_task():
        await asyncio.sleep(0.03)
        return "ok"

    async def failing_task():
        await asyncio.sleep(0.03)
        raise ValueError("failure")

    futures = []

    for _ in range(5):
        futures.append(
            executor.submit(successful_task())
        )
        futures.append(
            executor.submit(failing_task())
        )

    successes = 0
    failures = 0

    for future in futures:
        try:
            result = future.result(timeout=5)

            assert result == "ok"
            successes += 1

        except ValueError as exc:
            assert str(exc) == "failure"
            failures += 1

    assert successes == 5
    assert failures == 5

    assert executor.running == 0
    assert executor.available == executor.max_tasks


def test_cancelled_task_releases_execution_slot(executor):
    async def long_running_task():
        await asyncio.sleep(30)

    future = executor.submit(long_running_task())

    for _ in range(100):
        if executor.running == 1:
            break

        import time
        time.sleep(0.01)

    assert executor.running == 1

    assert future.cancel()

    with pytest.raises(CancelledError):
        future.result(timeout=5)

    for _ in range(100):
        if executor.running == 0:
            break

        import time
        time.sleep(0.01)

    assert executor.running == 0
    assert executor.available == executor.max_tasks


def test_executor_recovers_after_cancellation(executor):
    async def long_running_task():
        await asyncio.sleep(30)

    async def successful_task():
        return "success"

    future = executor.submit(long_running_task())

    for _ in range(100):
        if executor.running == 1:
            break

        import time
        time.sleep(0.01)

    assert executor.running == 1

    assert future.cancel()

    with pytest.raises(CancelledError):
        future.result(timeout=5)

    for _ in range(100):
        if executor.running == 0:
            break

        import time
        time.sleep(0.01)

    assert executor.running == 0
    assert executor.available == executor.max_tasks

    result = executor.submit(successful_task())

    assert result.result(timeout=5) == "success"

    assert executor.running == 0
    assert executor.available == executor.max_tasks
