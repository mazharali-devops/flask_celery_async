import time


def test_cancellation_reaches_running_coroutine():
    executor = AsyncExecutor(
        max_tasks=1,
        thread_name="test-cancellation",
    )

    executor.start()

    started = threading.Event()
    cancelled = threading.Event()

    async def cancellable():
        started.set()

        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    try:
        future = executor.submit(cancellable())

        assert started.wait(timeout=5)

        assert future.cancel() is True

        with pytest.raises(CancelledError):
            future.result(timeout=5)

        assert cancelled.wait(timeout=5), (
            "Coroutine did not receive asyncio.CancelledError"
        )

    finally:
        executor.shutdown(wait=True, timeout=5)


from concurrent.futures import CancelledError


def test_cancellation_releases_executor_slot():
    executor = AsyncExecutor(
        max_tasks=1,
        thread_name="test-cancellation-slot",
    )

    executor.start()

    started = threading.Event()

    async def cancellable():
        started.set()

        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            raise

    async def follow_up():
        return "ok"

    try:
        first = executor.submit(cancellable())

        assert started.wait(timeout=5)

        # The only execution slot is occupied.
        assert executor.running == 1
        assert executor.available == 0

        assert first.cancel() is True

        with pytest.raises(CancelledError):
            first.result(timeout=5)

        # Give the asyncio loop time to finish the cancellation cleanup.
        deadline = threading.Event()

        for _ in range(500):
            if executor.running == 0 and executor.available == 1:
                break
            deadline.wait(0.01)

        assert executor.running == 0
        assert executor.available == 1

        # The executor must still accept another coroutine.
        second = executor.submit(follow_up())

        assert second.result(timeout=5) == "ok"

    finally:
        executor.shutdown(wait=True, timeout=5)


import asyncio
import threading

import pytest

from flask_async_celery.executor import AsyncExecutor


def test_shutdown_waits_for_running_task():
    executor = AsyncExecutor(
        max_tasks=1,
        thread_name="test-shutdown",
    )

    executor.start()

    started = threading.Event()
    finished = threading.Event()

    async def long_running():
        started.set()

        try:
            await asyncio.sleep(0.2)
        finally:
            finished.set()

    try:
        future = executor.submit(long_running())

        assert started.wait(timeout=5)

        executor.shutdown(wait=True, timeout=5)

        assert finished.is_set()
        with pytest.raises(CancelledError):
            future.result(timeout=1)
        assert executor.running == 0
        assert executor.available == 1
        assert executor.is_running is False

    finally:
        # Safe even if shutdown() was already called.
        executor.shutdown(wait=True, timeout=5)


def test_shutdown_now_stops_executor():
    executor = AsyncExecutor(
        max_tasks=1,
        thread_name="test-shutdown-now",
    )

    executor.start()

    started = threading.Event()
    cancelled = threading.Event()

    async def long_running():
        started.set()

        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    try:
        future = executor.submit(long_running())

        assert started.wait(timeout=5)

        executor.shutdown(wait=False)

        assert executor.is_running is False

        # The coroutine should receive cancellation.
        assert cancelled.wait(timeout=5)

        with pytest.raises(CancelledError):
            future.result(timeout=5)

        assert executor.running == 0
        assert executor.available == 1

    finally:
        executor.shutdown(wait=False)


def test_cancel_task():
    executor = AsyncExecutor(
        max_tasks=1,
        slow_task_threshold=1,
    )
    executor.start()

    async def cancellable_task():
        await asyncio.sleep(60)

    future = executor.submit(
        cancellable_task(),
        task_id="cancel-1",
        task_name="test.cancellable",
    )

    try:
        time.sleep(0.1)

        assert executor.cancel_task("cancel-1") is True

        with pytest.raises(CancelledError):
            future.result(timeout=2)

        stats = executor.stats()

        assert stats["cancelled"] == 1
        assert stats["running"] == 0
        assert stats["available"] == 1
        assert stats["total"] == 1

        assert executor.cancel_task("cancel-1") is False
        assert executor.cancel_task("does-not-exist") is False

    finally:
        executor.shutdown()


def test_cancellation_of_queued_task():
    executor = AsyncExecutor(
        max_tasks=1,
        thread_name="test-cancellation-queued",
    )

    executor.start()

    first_started = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()

    async def first_task():
        first_started.set()
        try:
            await asyncio.to_thread(release_first.wait)
        finally:
            pass

    async def second_task():
        second_started.set()
        return "should-not-run"

    try:
        first = executor.submit(
            first_task(),
            task_id="queued-first",
            task_name="test.first",
        )

        assert first_started.wait(timeout=5)

        # The only slot is occupied, so this task must wait.
        second = executor.submit(
            second_task(),
            task_id="queued-second",
            task_name="test.second",
        )

        # Give the event loop a chance to enqueue the second task.
        time.sleep(0.1)

        assert executor.running == 1
        assert second_started.is_set() is False

        # The queued task should now be cancellable even though
        # it has not acquired the semaphore yet.
        assert executor.cancel_task("queued-second") is True

        with pytest.raises(CancelledError):
            second.result(timeout=5)

        assert second_started.is_set() is False

        # The running task is still alive.
        assert executor.running == 1

        # Release the first task.
        release_first.set()

        assert first.result(timeout=5) is None

        # The cancelled queued task must not consume a slot or
        # remain registered.
        deadline = time.monotonic() + 5

        while time.monotonic() < deadline:
            stats = executor.stats()

            if (
                    stats["running"] == 0
                    and stats["available"] == 1
                    and stats["cancelled"] == 1
            ):
                break

            time.sleep(0.01)

        stats = executor.stats()

        assert stats["running"] == 0
        assert stats["available"] == 1
        assert stats["cancelled"] == 1
        assert stats["total"] == 2

    finally:
        release_first.set()
        executor.shutdown(wait=True, timeout=5)



def test_cancel_completed_task():
    executor = AsyncExecutor(
        max_tasks=1,
        thread_name="test-cancel-completed",
    )

    executor.start()

    async def completed_task():
        return "done"

    try:
        future = executor.submit(
            completed_task(),
            task_id="completed-1",
            task_name="test.completed",
        )

        assert future.result(timeout=5) == "done"

        # Wait until executor accounting has finished.
        deadline = time.monotonic() + 5

        while time.monotonic() < deadline:
            if executor.stats()["completed"] == 1:
                break
            time.sleep(0.01)

        stats = executor.stats()

        assert stats["completed"] == 1
        assert stats["cancelled"] == 0
        assert stats["running"] == 0
        assert stats["available"] == 1

        # A completed task must no longer be cancellable.
        assert executor.cancel_task("completed-1") is False

    finally:
        executor.shutdown(wait=True, timeout=5)


def test_cancel_task_after_shutdown():
    executor = AsyncExecutor(
        max_tasks=1,
        thread_name="test-cancel-after-shutdown",
    )

    executor.start()

    async def long_running():
        await asyncio.sleep(30)

    try:
        future = executor.submit(
            long_running(),
            task_id="shutdown-cancel-1",
            task_name="test.shutdown_cancel",
        )

        time.sleep(0.1)

        executor.shutdown(wait=True, timeout=5)

        assert executor.is_running is False

        # The executor is already stopped, so cancellation must
        # not report success.
        assert executor.cancel_task("shutdown-cancel-1") is False

        with pytest.raises(CancelledError):
            future.result(timeout=1)

    finally:
        executor.shutdown(wait=True, timeout=5)
