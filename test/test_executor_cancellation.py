import asyncio
import threading

import pytest
from concurrent.futures import CancelledError

from flask_async_celery.executor import AsyncExecutor
from concurrent.futures import CancelledError

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


import asyncio
import threading

import pytest
from concurrent.futures import CancelledError

from flask_async_celery.executor import AsyncExecutor


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