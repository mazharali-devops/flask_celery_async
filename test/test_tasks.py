import asyncio

import pytest

from flask_async_celery.executor import AsyncExecutor
from flask_async_celery.task import (
    AsyncTask,
    reset_async_executor,
    set_async_executor,
)


class TestAsyncTask(AsyncTask):
    name = "test.async_failure"

    async def run(self):
        await asyncio.sleep(0.1)
        raise ValueError("async task failed")


def test_async_task_exception():
    executor = AsyncExecutor(max_tasks=3)
    executor.start()

    token = set_async_executor(executor)

    try:
        task = TestAsyncTask()

        with pytest.raises(ValueError, match="async task failed"):
            task()

    finally:
        reset_async_executor(token)
        executor.shutdown()


import asyncio

import pytest

from flask_async_celery.executor import AsyncExecutor
from flask_async_celery.task import (
    AsyncTask,
    reset_async_executor,
    set_async_executor,
)


class RetryAsyncTask(AsyncTask):
    name = "test.async_retry"

    def __init__(self):
        self.attempts = 0

    async def run(self):
        self.attempts += 1
        await asyncio.sleep(0.1)

        if self.attempts == 1:
            raise self.retry(
                exc=ValueError("temporary failure"),
                countdown=0,
            )

        return "success"


def test_async_task_retry():
    executor = AsyncExecutor(max_tasks=3)
    executor.start()

    token = set_async_executor(executor)

    try:
        task = RetryAsyncTask()

        with pytest.raises(Exception):
            task()

    finally:
        reset_async_executor(token)
        executor.shutdown()


