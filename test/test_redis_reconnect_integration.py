from __future__ import annotations

import asyncio
import subprocess
import time

from celery import Celery
from celery.contrib.testing.worker import start_worker

from flask_async_celery.task import AsyncTask

BROKER_URL = "redis://127.0.0.1:6381/0"
# BACKEND_URL = "redis://127.0.0.1:6381/1"
BACKEND_URL = "cache+memory://"
REDIS_PORT = 6381


def _start_redis() -> subprocess.Popen:
    """Start an isolated Redis instance for this test."""
    process = subprocess.Popen(
        [
            "redis-server",
            "--port",
            str(REDIS_PORT),
            "--save",
            "",
            "--appendonly",
            "no",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    deadline = time.monotonic() + 10

    while time.monotonic() < deadline:
        check = subprocess.run(
            [
                "redis-cli",
                "-p",
                str(REDIS_PORT),
                "ping",
            ],
            capture_output=True,
            text=True,
        )

        if check.returncode == 0 and check.stdout.strip() == "PONG":
            return process

        time.sleep(0.2)

    process.terminate()

    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)

    raise AssertionError(
        f"Redis did not become available on port {REDIS_PORT}"
    )


def _stop_redis(process: subprocess.Popen) -> None:
    """Stop the Redis process owned by this test."""
    if process.poll() is not None:
        return

    process.terminate()

    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _wait_for_redis(timeout: float = 10) -> None:
    """Wait until Redis accepts connections."""
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        check = subprocess.run(
            [
                "redis-cli",
                "-p",
                str(REDIS_PORT),
                "ping",
            ],
            capture_output=True,
            text=True,
        )

        if check.returncode == 0 and check.stdout.strip() == "PONG":
            return

        time.sleep(0.2)

    raise AssertionError(
        f"Redis did not become available on port {REDIS_PORT}"
    )


def test_worker_recovers_after_redis_restart():
    app = Celery(
        "redis_reconnect_test",
        broker=BROKER_URL,
        backend=BACKEND_URL,
    )

    app.conf.update(
        task_always_eager=False,
        task_track_started=True,

        worker_pool="flask_async_celery.pool:AsyncIOPool",
        worker_concurrency=5,
        worker_prefetch_multiplier=1,
        worker_disable_prefetch=True,
        worker_pool_putlocks=False,
        worker_cancel_long_running_tasks_on_connection_loss=False,

        # Reconnect configuration.
        broker_connection_retry=True,
        broker_connection_retry_on_startup=True,
        broker_connection_max_retries=None,
        broker_connection_retry_interval=1,
    )

    @app.task(
        name="test.redis_reconnect_task",
        base=AsyncTask,
    )
    async def reconnect_task(value):
        await asyncio.sleep(0.05)
        return value

    redis_process: subprocess.Popen | None = None

    try:
        # --------------------------------------------------
        # Start dedicated Redis instance.
        # --------------------------------------------------

        print("\n>>> STARTING TEST REDIS")

        redis_process = _start_redis()

        print(">>> TEST REDIS AVAILABLE")

        # --------------------------------------------------
        # Start Celery worker.
        # --------------------------------------------------

        print(">>> STARTING CELERY WORKER")

        with start_worker(
                app,
                concurrency=5,
                pool="flask_async_celery.pool:AsyncIOPool",
                loglevel="INFO",
                perform_ping_check=False,
        ):
            # --------------------------------------------------
            # Phase 1: prove the worker is processing tasks.
            # --------------------------------------------------

            print(">>> PHASE 1: INITIAL TASK")

            result = reconnect_task.delay("before-reconnect")

            assert result.get(timeout=10) == "before-reconnect"

            print(">>> INITIAL TASK COMPLETE")

            # --------------------------------------------------
            # Stop Redis.
            # --------------------------------------------------

            print(">>> STOPPING REDIS")

            _stop_redis(redis_process)
            redis_process = None

            print(">>> REDIS STOPPED")

            # Give the worker time to detect the broken
            # broker connection and enter its reconnect path.
            time.sleep(3)

            # --------------------------------------------------
            # Start a completely new Redis process.
            # --------------------------------------------------

            print(">>> STARTING REDIS AGAIN")

            redis_process = _start_redis()

            print(">>> REDIS AVAILABLE AGAIN")

            # Give Kombu/Celery some time to reconnect.
            time.sleep(2)

            # --------------------------------------------------
            # Phase 2: task after Redis reconnect.
            # --------------------------------------------------

            print(">>> PHASE 2: POST-RECONNECT TASK")

            started = time.monotonic()

            result = reconnect_task.delay("after-reconnect")

            value = result.get(timeout=15)

            elapsed = time.monotonic() - started

            print(f">>> POST-RECONNECT RESULT: {value}")
            print(
                f">>> POST-RECONNECT ELAPSED: "
                f"{elapsed:.3f}s"
            )

            assert value == "after-reconnect"

            # The important property is that the worker actually
            # recovered instead of remaining stuck behind a long
            # Hub/event-loop poll timeout.
            assert elapsed < 5

    finally:
        # --------------------------------------------------
        # Always clean up the Redis process owned by this test.
        # --------------------------------------------------

        if redis_process is not None:
            print(">>> STOPPING TEST REDIS")
            _stop_redis(redis_process)
