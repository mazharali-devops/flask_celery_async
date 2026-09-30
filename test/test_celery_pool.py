from __future__ import annotations

import asyncio
import time
from concurrent.futures import CancelledError
import pytest
from celery import Celery
import threading
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
    Verify that AsyncIOPool executes async targets concurrently.
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


def test_asyncio_pool_recovers_after_async_exception(celery_app):
    """
    Verify that an exception raised by one async target does not
    break the pool and that another task can execute afterward.
    """

    pool = AsyncIOPool(
        limit=2,
        app=celery_app,
    )

    pool.on_start()

    async def failing_target():
        await asyncio.sleep(0.05)
        raise RuntimeError("expected async failure")

    async def successful_target():
        await asyncio.sleep(0.05)
        return "success"

    try:
        failed = pool.on_apply(
            target=failing_target,
            args=(),
            callback=lambda value: None,
        )

        with pytest.raises(
                RuntimeError,
                match="expected async failure",
        ):
            failed.get(timeout=5)

        # The failed task must release its execution slot.
        assert pool.async_executor.running == 0
        assert pool.async_executor.available == 2

        # The pool must remain usable.
        successful = pool.on_apply(
            target=successful_target,
            args=(),
            callback=lambda value: None,
        )

        assert successful.get(timeout=5) == "success"

        assert pool.async_executor.running == 0
        assert pool.async_executor.available == 2

    finally:
        pool.on_stop()


def test_asyncio_pool_recovers_after_multiple_exceptions(celery_app):
    """
    Verify that multiple failed async targets do not deadlock
    or consume execution slots permanently.
    """

    pool = AsyncIOPool(
        limit=2,
        app=celery_app,
    )

    pool.on_start()

    async def failing_target(number):
        await asyncio.sleep(0.02)
        raise RuntimeError(f"failure-{number}")

    try:
        futures = [
            pool.on_apply(
                target=failing_target,
                args=(number,),
                callback=lambda value: None,
            )
            for number in range(10)
        ]

        for number, future in enumerate(futures):
            with pytest.raises(
                    RuntimeError,
                    match=f"failure-{number}",
            ):
                future.get(timeout=5)

        assert pool.async_executor.running == 0
        assert pool.async_executor.available == 2

    finally:
        pool.on_stop()


def test_asyncio_pool_success_after_multiple_exceptions(celery_app):
    """
    Verify that the pool continues executing tasks after several
    failed tasks.
    """

    pool = AsyncIOPool(
        limit=2,
        app=celery_app,
    )

    pool.on_start()

    async def failing_target():
        raise RuntimeError("expected failure")

    async def successful_target():
        return "success"

    try:
        failed_futures = [
            pool.on_apply(
                target=failing_target,
                args=(),
                callback=lambda value: None,
            )
            for _ in range(5)
        ]

        for future in failed_futures:
            with pytest.raises(
                    RuntimeError,
                    match="expected failure",
            ):
                future.get(timeout=5)

        assert pool.async_executor.running == 0

        successful_futures = [
            pool.on_apply(
                target=successful_target,
                args=(),
                callback=lambda value: None,
            )
            for _ in range(5)
        ]

        results = [
            future.get(timeout=5)
            for future in successful_futures
        ]

        assert results == [
            "success",
            "success",
            "success",
            "success",
            "success",
        ]

        assert pool.async_executor.running == 0
        assert pool.async_executor.available == 2

    finally:
        pool.on_stop()


def test_asyncio_pool_graceful_shutdown_cancels_pending_tasks(celery_app):
    """
    Verify that on_stop() shuts down the asyncio loop and cancels
    pending async work without leaving the executor thread running.
    """

    pool = AsyncIOPool(
        limit=2,
        app=celery_app,
    )

    pool.on_start()

    async def long_running_task():
        await asyncio.sleep(60)
        return "finished"

    try:
        futures = [
            pool.on_apply(
                target=long_running_task,
                args=(),
                callback=lambda value: None,
            )
            for _ in range(2)
        ]

        # Give the tasks enough time to actually start.
        deadline = time.monotonic() + 5

        while (
                pool.async_executor.running < 2
                and time.monotonic() < deadline
        ):
            time.sleep(0.01)

        assert pool.async_executor.running == 2

    finally:
        pool.on_stop()

    assert not pool.async_executor.is_running
    assert pool.async_executor.loop is None

    assert pool.executor._shutdown

    for future in futures:
        assert future.ready()


def test_asyncio_pool_terminate_stops_executor(celery_app):
    """
    Verify that on_terminate() requests asyncio shutdown and that
    the asyncio executor eventually stops.
    """

    pool = AsyncIOPool(
        limit=2,
        app=celery_app,
    )

    pool.on_start()

    async def long_running_task():
        await asyncio.sleep(60)
        return "finished"

    try:
        futures = [
            pool.on_apply(
                target=long_running_task,
                args=(),
                callback=lambda value: None,
            )
            for _ in range(2)
        ]

        # Make sure both tasks have actually started.
        deadline = time.monotonic() + 5

        while (
                pool.async_executor.running < 2
                and time.monotonic() < deadline
        ):
            time.sleep(0.01)

        assert pool.async_executor.running == 2

        # Termination must request shutdown.
        pool.on_terminate()

        # on_terminate() uses wait=False, so allow the asyncio
        # thread a bounded amount of time to finish.
        deadline = time.monotonic() + 5

        while (
                pool.async_executor.loop is not None
                and time.monotonic() < deadline
        ):
            time.sleep(0.01)

        assert pool.async_executor.loop is None
        assert not pool.async_executor.is_running
        assert pool.executor._shutdown

    finally:
        # on_terminate() may already have shut everything down.
        # Do not call on_stop() here because termination is the
        # terminal lifecycle path.
        pass


def test_asyncio_pool_stress_never_exceeds_max_tasks(celery_app):
    """
    Submit many async tasks and verify that actual async concurrency
    never exceeds the configured max_tasks.
    """

    max_tasks = 5
    total_tasks = 50

    pool = AsyncIOPool(
        limit=max_tasks,
        app=celery_app,
    )

    pool.on_start()

    state = {
        "running": 0,
        "maximum": 0,
    }

    async def workload(number):
        state["running"] += 1
        state["maximum"] = max(
            state["maximum"],
            state["running"],
        )

        try:
            await asyncio.sleep(0.1)
            return number
        finally:
            state["running"] -= 1

    try:
        futures = [
            pool.on_apply(
                target=workload,
                args=(number,),
                callback=lambda value: None,
            )
            for number in range(total_tasks)
        ]

        results = [
            future.get(timeout=10)
            for future in futures
        ]

        assert results == list(range(total_tasks))

        assert state["running"] == 0

        # This is the important assertion.
        assert state["maximum"] <= max_tasks

        assert pool.async_executor.running == 0
        assert pool.async_executor.available == max_tasks

    finally:
        pool.on_stop()


def test_asyncio_pool_stress_500_tasks(celery_app):
    """
    Submit 500 async tasks with a concurrency limit of 5.

    Verifies that a large queued workload completes without
    exceeding the configured concurrency limit.
    """

    max_tasks = 5
    total_tasks = 500

    pool = AsyncIOPool(
        limit=max_tasks,
        app=celery_app,
    )

    pool.on_start()

    state = {
        "running": 0,
        "maximum": 0,
    }

    async def workload(number):
        state["running"] += 1
        state["maximum"] = max(
            state["maximum"],
            state["running"],
        )

        try:
            await asyncio.sleep(0.01)
            return number
        finally:
            state["running"] -= 1

    try:
        futures = [
            pool.on_apply(
                target=workload,
                args=(number,),
                callback=lambda value: None,
            )
            for number in range(total_tasks)
        ]

        results = [
            future.get(timeout=30)
            for future in futures
        ]

        assert results == list(range(total_tasks))

        assert state["running"] == 0
        assert state["maximum"] <= max_tasks

        assert pool.async_executor.running == 0
        assert pool.async_executor.available == max_tasks

    finally:
        pool.on_stop()


def test_asyncio_pool_exception_storm_500_tasks(celery_app):
    """
    Submit 500 failing async tasks and verify that exceptions do not
    leak execution slots or break the pool.
    """

    max_tasks = 5
    total_tasks = 500

    pool = AsyncIOPool(
        limit=max_tasks,
        app=celery_app,
    )

    pool.on_start()

    async def failing_task(number):
        await asyncio.sleep(0.01)
        raise RuntimeError(f"expected failure {number}")

    async def successful_task():
        return "recovered"

    try:
        futures = [
            pool.on_apply(
                target=failing_task,
                args=(number,),
                callback=lambda value: None,
            )
            for number in range(total_tasks)
        ]

        for number, future in enumerate(futures):
            with pytest.raises(
                    RuntimeError,
                    match=f"expected failure {number}",
            ):
                future.get(timeout=30)

        # Every failed task must have released its slot.
        assert pool.async_executor.running == 0
        assert pool.async_executor.available == max_tasks

        # Most important: the pool must still be usable.
        recovered = pool.on_apply(
            target=successful_task,
            args=(),
            callback=lambda value: None,
        )

        assert recovered.get(timeout=5) == "recovered"

        assert pool.async_executor.running == 0
        assert pool.async_executor.available == max_tasks

    finally:
        pool.on_stop()


def test_asyncio_pool_mixed_500_tasks(celery_app):
    """
    Submit a mixed workload of successful and failing async tasks.

    Verifies that failures do not interfere with successful tasks and
    that the concurrency limit remains enforced.
    """

    max_tasks = 5
    total_tasks = 500

    pool = AsyncIOPool(
        limit=max_tasks,
        app=celery_app,
    )

    pool.on_start()

    state = {
        "running": 0,
        "maximum": 0,
    }

    async def mixed_task(number):
        state["running"] += 1
        state["maximum"] = max(
            state["maximum"],
            state["running"],
        )

        try:
            await asyncio.sleep(0.01)

            if number % 3 == 0:
                raise RuntimeError(f"expected failure {number}")

            return number

        finally:
            state["running"] -= 1

    try:
        futures = [
            pool.on_apply(
                target=mixed_task,
                args=(number,),
                callback=lambda value: None,
            )
            for number in range(total_tasks)
        ]

        successful = []
        failed = []

        for number, future in enumerate(futures):
            try:
                result = future.get(timeout=30)
                successful.append(result)
            except RuntimeError as exc:
                assert str(exc) == f"expected failure {number}"
                failed.append(number)

        expected_failed = [
            number
            for number in range(total_tasks)
            if number % 3 == 0
        ]

        expected_successful = [
            number
            for number in range(total_tasks)
            if number % 3 != 0
        ]

        assert failed == expected_failed
        assert successful == expected_successful

        assert state["running"] == 0
        assert state["maximum"] <= max_tasks

        assert pool.async_executor.running == 0
        assert pool.async_executor.available == max_tasks

        # Verify the pool is still usable after the mixed workload.
        async def recovery_task():
            return "recovered"

        recovery = pool.on_apply(
            target=recovery_task,
            args=(),
            callback=lambda value: None,
        )

        assert recovery.get(timeout=5) == "recovered"

        assert pool.async_executor.running == 0
        assert pool.async_executor.available == max_tasks

    finally:
        pool.on_stop()


def test_asyncio_pool_large_queue_remains_bounded(celery_app):
    """
    Submit a large number of async tasks and verify that actual
    execution remains bounded by max_tasks.
    """

    max_tasks = 5
    total_tasks = 2000

    pool = AsyncIOPool(
        limit=max_tasks,
        app=celery_app,
    )

    pool.on_start()

    async def workload(number):
        await asyncio.sleep(0.01)
        return number

    try:
        futures = [
            pool.on_apply(
                target=workload,
                args=(number,),
                callback=lambda value: None,
            )
            for number in range(total_tasks)
        ]

        # The pool must never execute more than max_tasks.
        assert pool.async_executor.running <= max_tasks

        results = [
            future.get(timeout=60)
            for future in futures
        ]

        assert results == list(range(total_tasks))

        assert pool.async_executor.running == 0
        assert pool.async_executor.available == max_tasks

    finally:
        pool.on_stop()


def test_asyncio_pool_memory_pressure(celery_app):
    """
    Verify bounded concurrency under memory-heavy async workloads.

    This is a diagnostic stress test rather than a strict RSS threshold
    test because Python's allocator may retain memory after objects are
    released.
    """
    import gc
    import os

    import psutil

    max_tasks = 5
    total_tasks = 100
    allocation_mb = 20

    pool = AsyncIOPool(
        limit=max_tasks,
        app=celery_app,
    )

    pool.on_start()

    process = psutil.Process(os.getpid())

    state = {
        "running": 0,
        "maximum": 0,
    }

    async def memory_task(number):
        state["running"] += 1
        state["maximum"] = max(
            state["maximum"],
            state["running"],
        )

        try:
            # Allocate approximately allocation_mb MB.
            data = bytearray(allocation_mb * 1024 * 1024)

            # Touch the memory so the OS actually backs the pages.
            for offset in range(0, len(data), 4096):
                data[offset] = number % 256

            await asyncio.sleep(0.05)

            return number

        finally:
            state["running"] -= 1

    try:
        gc.collect()

        before = process.memory_info().rss

        futures = [
            pool.on_apply(
                target=memory_task,
                args=(number,),
                callback=lambda value: None,
            )
            for number in range(total_tasks)
        ]

        # While the workload is running, only max_tasks should execute.
        peak = before

        while not all(future.ready() for future in futures):
            current = process.memory_info().rss
            peak = max(peak, current)

            assert state["running"] <= max_tasks

            time.sleep(0.01)

        results = [
            future.get(timeout=30)
            for future in futures
        ]

        after = process.memory_info().rss

        assert results == list(range(total_tasks))

        assert state["running"] == 0
        assert state["maximum"] <= max_tasks

        assert pool.async_executor.running == 0
        assert pool.async_executor.available == max_tasks

        print(
            f"\nRSS before: {before / 1024 / 1024:.1f} MB"
            f"\nRSS peak:   {peak / 1024 / 1024:.1f} MB"
            f"\nRSS after:  {after / 1024 / 1024:.1f} MB"
            f"\nRSS peak increase: "
            f"{(peak - before) / 1024 / 1024:.1f} MB"
            f"\nRSS after increase: "
            f"{(after - before) / 1024 / 1024:.1f} MB"
            f"\nMaximum async concurrency: {state['maximum']}"
        )

    finally:
        pool.on_stop()


def test_asyncio_pool_repeated_memory_workloads(celery_app):
    """
    Run repeated memory-heavy workloads and observe whether RSS
    continues growing between batches.

    This is a diagnostic test. Python's allocator may retain memory,
    so the test does not require RSS to return to the original value.
    """

    import gc
    import os

    import psutil

    max_tasks = 5
    tasks_per_batch = 100
    batches = 10
    allocation_mb = 20

    pool = AsyncIOPool(
        limit=max_tasks,
        app=celery_app,
    )

    pool.on_start()

    process = psutil.Process(os.getpid())

    async def memory_task(number):
        data = bytearray(allocation_mb * 1024 * 1024)

        for offset in range(0, len(data), 4096):
            data[offset] = number % 256

        await asyncio.sleep(0.02)

        return number

    try:
        gc.collect()

        baseline = process.memory_info().rss

        rss_values = []

        for batch in range(batches):
            futures = [
                pool.on_apply(
                    target=memory_task,
                    args=(number,),
                    callback=lambda value: None,
                )
                for number in range(tasks_per_batch)
            ]

            results = [
                future.get(timeout=30)
                for future in futures
            ]

            assert results == list(range(tasks_per_batch))

            gc.collect()

            rss = process.memory_info().rss
            rss_values.append(rss)

            print(
                f"\nBatch {batch + 1}: "
                f"{rss / 1024 / 1024:.1f} MB "
                f"(+{(rss - baseline) / 1024 / 1024:.1f} MB)"
            )

        assert pool.async_executor.running == 0
        assert pool.async_executor.available == max_tasks

        print("\nRSS summary:")

        for index, rss in enumerate(rss_values, start=1):
            print(
                f"  Batch {index}: "
                f"{rss / 1024 / 1024:.1f} MB"
            )

    finally:
        pool.on_stop()


def test_asyncio_pool_memory_pressure_with_failures(celery_app):
    """
    Combine memory-heavy workloads with task failures.

    Verifies that failed memory-heavy tasks release their execution
    slots and that repeated batches do not continuously increase RSS.
    """

    import gc
    import os

    import psutil

    max_tasks = 5
    tasks_per_batch = 100
    batches = 5
    allocation_mb = 20

    pool = AsyncIOPool(
        limit=max_tasks,
        app=celery_app,
    )

    pool.on_start()

    process = psutil.Process(os.getpid())

    state = {
        "running": 0,
        "maximum": 0,
    }

    async def memory_task(number):
        state["running"] += 1
        state["maximum"] = max(
            state["maximum"],
            state["running"],
        )

        try:
            data = bytearray(
                allocation_mb * 1024 * 1024
            )

            for offset in range(0, len(data), 4096):
                data[offset] = number % 256

            await asyncio.sleep(0.02)

            if number % 3 == 0:
                raise RuntimeError(
                    f"expected memory failure {number}"
                )

            return number

        finally:
            state["running"] -= 1

    try:
        gc.collect()

        baseline = process.memory_info().rss
        rss_values = []

        for batch in range(batches):
            futures = [
                pool.on_apply(
                    target=memory_task,
                    args=(number,),
                    callback=lambda value: None,
                )
                for number in range(tasks_per_batch)
            ]

            successful = []
            failed = []

            for number, future in enumerate(futures):
                try:
                    successful.append(
                        future.get(timeout=30)
                    )
                except RuntimeError as exc:
                    assert str(exc) == (
                        f"expected memory failure {number}"
                    )
                    failed.append(number)

            expected_failed = [
                number
                for number in range(tasks_per_batch)
                if number % 3 == 0
            ]

            expected_successful = [
                number
                for number in range(tasks_per_batch)
                if number % 3 != 0
            ]

            assert failed == expected_failed
            assert successful == expected_successful

            assert state["running"] == 0
            assert state["maximum"] <= max_tasks

            assert pool.async_executor.running == 0
            assert pool.async_executor.available == max_tasks

            gc.collect()

            rss = process.memory_info().rss
            rss_values.append(rss)

            print(
                f"\nBatch {batch + 1}: "
                f"{rss / 1024 / 1024:.1f} MB "
                f"(+{(rss - baseline) / 1024 / 1024:.1f} MB)"
            )

        # The pool must remain usable after all failures.
        async def recovery_task():
            return "recovered"

        recovery = pool.on_apply(
            target=recovery_task,
            args=(),
            callback=lambda value: None,
        )

        assert recovery.get(timeout=5) == "recovered"

        assert pool.async_executor.running == 0
        assert pool.async_executor.available == max_tasks

        print("\nRSS summary:")

        for index, rss in enumerate(rss_values, start=1):
            print(
                f"  Batch {index}: "
                f"{rss / 1024 / 1024:.1f} MB"
            )

    finally:
        pool.on_stop()


def test_asyncio_pool_repeated_exception_batches(celery_app):
    """
    Run repeated batches of failing async tasks and observe RSS.

    This isolates exception-related memory retention from the memory
    allocation performed by the task itself.
    """

    import gc
    import os

    import psutil

    max_tasks = 5
    tasks_per_batch = 100
    batches = 10

    pool = AsyncIOPool(
        limit=max_tasks,
        app=celery_app,
    )

    pool.on_start()

    process = psutil.Process(os.getpid())

    async def failing_task(number):
        await asyncio.sleep(0.01)
        raise RuntimeError(
            f"expected failure {number}"
        )

    try:
        gc.collect()

        baseline = process.memory_info().rss
        rss_values = []

        for batch in range(batches):
            futures = [
                pool.on_apply(
                    target=failing_task,
                    args=(number,),
                    callback=lambda value: None,
                )
                for number in range(tasks_per_batch)
            ]

            for number, future in enumerate(futures):
                with pytest.raises(
                        RuntimeError,
                        match=f"expected failure {number}",
                ):
                    future.get(timeout=30)

            gc.collect()

            rss = process.memory_info().rss
            rss_values.append(rss)

            print(
                f"\nBatch {batch + 1}: "
                f"{rss / 1024 / 1024:.1f} MB "
                f"(+{(rss - baseline) / 1024 / 1024:.1f} MB)"
            )

            assert pool.async_executor.running == 0
            assert pool.async_executor.available == max_tasks

        print("\nRSS summary:")

        for index, rss in enumerate(rss_values, start=1):
            print(
                f"  Batch {index}: "
                f"{rss / 1024 / 1024:.1f} MB"
            )

    finally:
        pool.on_stop()


def test_asyncio_pool_memory_failures_without_future_retention(celery_app):
    """
    Diagnostic test for memory-heavy failures.

    Each Future is consumed immediately so failed Future/exception
    objects are not intentionally retained by the test.
    """

    import gc
    import os

    import psutil

    max_tasks = 5
    tasks_per_batch = 100
    batches = 5
    allocation_mb = 20

    pool = AsyncIOPool(
        limit=max_tasks,
        app=celery_app,
    )

    pool.on_start()

    process = psutil.Process(os.getpid())

    async def memory_failing_task(number):
        data = bytearray(
            allocation_mb * 1024 * 1024
        )

        for offset in range(0, len(data), 4096):
            data[offset] = number % 256

        await asyncio.sleep(0.02)

        raise RuntimeError(
            f"expected memory failure {number}"
        )

    try:
        gc.collect()

        baseline = process.memory_info().rss
        rss_values = []

        for batch in range(batches):
            for number in range(tasks_per_batch):
                future = pool.on_apply(
                    target=memory_failing_task,
                    args=(number,),
                    callback=lambda value: None,
                )

                with pytest.raises(
                        RuntimeError,
                        match=f"expected memory failure {number}",
                ):
                    future.get(timeout=30)

                # Explicitly remove our reference to the failed result.
                del future

            gc.collect()

            rss = process.memory_info().rss
            rss_values.append(rss)

            print(
                f"\nBatch {batch + 1}: "
                f"{rss / 1024 / 1024:.1f} MB "
                f"(+{(rss - baseline) / 1024 / 1024:.1f} MB)"
            )

            assert pool.async_executor.running == 0
            assert pool.async_executor.available == max_tasks

        print("\nRSS summary:")

        for index, rss in enumerate(rss_values, start=1):
            print(
                f"  Batch {index}: "
                f"{rss / 1024 / 1024:.1f} MB"
            )

    finally:
        pool.on_stop()



def test_asyncio_pool_terminate_job_cancels_task(celery_app):
    """
    Verify that terminating an individual pool task propagates
    cancellation to the asyncio coroutine.
    """

    pool = AsyncIOPool(
        limit=1,
        app=celery_app,
    )

    pool.on_start()

    started = threading.Event()
    cancelled = threading.Event()

    async def long_running_task():
        started.set()

        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    try:
        task_id = "pool-terminate-1"

        result = pool.on_apply(
            target=long_running_task,
            args=(),
            kwargs={},
            callback=lambda value: None,
            correlation_id=task_id,
        )

        assert started.wait(timeout=5)

        assert pool.async_executor.running == 1
        assert pool.async_executor.available == 0

        # This is the same path used by Celery's Request.terminate().
        assert result.terminate(signal=15) is True

        with pytest.raises(CancelledError):
            result.get(timeout=5)

        assert cancelled.wait(timeout=5)

        deadline = time.monotonic() + 5

        while time.monotonic() < deadline:
            if (
                pool.async_executor.running == 0
                and pool.async_executor.available == 1
            ):
                break

            time.sleep(0.01)

        assert pool.async_executor.running == 0
        assert pool.async_executor.available == 1

    finally:
        pool.on_stop()
