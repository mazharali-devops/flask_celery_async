from __future__ import annotations

import asyncio
from flask import request
import pytest
from celery.contrib.testing.worker import start_worker
from flask import Flask, current_app
import threading
from flask_async_celery import AsyncCelery


@pytest.fixture
def flask_app():
    app = Flask("flask_async_test")

    async_celery = AsyncCelery(
        app,
        broker_url="redis://127.0.0.1:6379/0",
        result_backend="redis://127.0.0.1:6379/1",
        max_tasks=5,
        disable_prefetch=True,
    )

    @async_celery.task(name="test.flask_app_context")
    async def flask_app_context_task():
        await asyncio.sleep(0.05)
        return current_app.name

    @async_celery.task(name="test.flask_request_context")
    async def flask_request_context_task():
        await asyncio.sleep(0.05)

        return {
            "path": request.path,
            "method": request.method,
            "query": request.args.get("name"),
        }

    @async_celery.task(name="test.flask_request_data")
    async def flask_request_data(path, method, query):
        await asyncio.sleep(0.05)

        return {
            "path": path,
            "method": method,
            "query": query,
        }

    @async_celery.task(name="test.flask_concurrent_context")
    async def flask_concurrent_context_task(value):
        await asyncio.sleep(0.05)

        return {
            "value": value,
            "app": current_app.name,
        }

    @async_celery.task(name="test.async_failure")
    async def async_failure_task():
        await asyncio.sleep(0.05)
        raise ValueError("intentional async failure")

    @async_celery.task(name="test.async_after_failure")
    async def async_after_failure_task():
        await asyncio.sleep(0.05)
        return "still working"

    @async_celery.task(name="test.concurrent_failure")
    async def concurrent_failure_task(value):
        await asyncio.sleep(0.05)

        if value % 2 == 0:
            raise ValueError(f"failed: {value}")

        return f"success: {value}"

    @async_celery.task(name="test.cancellable")
    async def cancellable_task():
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            return "cancelled"

    app.extensions["cancellable_task"] = cancellable_task
    app.extensions["concurrent_failure_task"] = concurrent_failure_task
    app.extensions["flask_concurrent_context_task"] = (
        flask_concurrent_context_task
    )
    app.extensions["async_failure_task"] = async_failure_task
    app.extensions["async_after_failure_task"] = async_after_failure_task
    app.extensions["async_celery"] = async_celery
    app.extensions["flask_app_context_task"] = flask_app_context_task
    app.extensions["flask_request_context_task"] = flask_request_context_task
    app.extensions["flask_request_data"] = flask_request_data

    return app


def test_async_celery_task_uses_async_task_by_default():
    from flask import Flask

    from flask_async_celery import AsyncCelery
    from flask_async_celery.task import AsyncTask

    app = Flask("test")

    async_celery = AsyncCelery(
        app,
        broker_url="redis://127.0.0.1:6379/0",
        result_backend="redis://127.0.0.1:6379/1",
    )

    @async_celery.task
    async def example_task():
        return "ok"

    assert isinstance(example_task, AsyncTask)

def test_flask_app_context(flask_app):
    async_celery = flask_app.extensions["async_celery"]
    task = flask_app.extensions["flask_app_context_task"]

    with flask_app.app_context():
        result = task.delay()

        with start_worker(
            async_celery.celery,
            concurrency=1,
            pool="flask_async_celery.pool:AsyncIOPool",
            loglevel="INFO",
            perform_ping_check=False,
        ):
            value = result.get(timeout=10)

    assert value == "flask_async_test"


def test_flask_request_data_can_be_passed_to_async_task(flask_app):
    async_celery = flask_app.extensions["async_celery"]
    task = flask_app.extensions["flask_request_data"]

    with flask_app.test_request_context(
        "/hello?name=Mazhar",
        method="GET",
    ):
        result = task.delay(
            path=request.path,
            method=request.method,
            query=request.args.get("name"),
        )

        with start_worker(
            async_celery.celery,
            concurrency=1,
            pool="flask_async_celery.pool:AsyncIOPool",
            loglevel="INFO",
            perform_ping_check=False,
        ):
            value = result.get(timeout=10)

    assert value == {
        "path": "/hello",
        "method": "GET",
        "query": "Mazhar",
    }


def test_flask_app_context_isolation_under_concurrency(flask_app):
    async_celery = flask_app.extensions["async_celery"]
    task = flask_app.extensions["flask_concurrent_context_task"]

    with start_worker(
        async_celery.celery,
        concurrency=5,
        pool="flask_async_celery.pool:AsyncIOPool",
        loglevel="INFO",
        perform_ping_check=False,
    ):
        results = [
            task.delay(i)
            for i in range(10)
        ]

        values = [
            result.get(timeout=10)
            for result in results
        ]

    assert values == [
        {
            "value": i,
            "app": "flask_async_test",
        }
        for i in range(10)
    ]

def test_async_task_exception_does_not_break_executor(flask_app):
    async_celery = flask_app.extensions["async_celery"]

    failure_task = flask_app.extensions["async_failure_task"]
    success_task = flask_app.extensions["async_after_failure_task"]

    with start_worker(
        async_celery.celery,
        concurrency=2,
        pool="flask_async_celery.pool:AsyncIOPool",
        loglevel="INFO",
        perform_ping_check=False,
    ):
        failed = failure_task.delay()

        with pytest.raises(ValueError, match="intentional async failure"):
            failed.get(timeout=10)

        successful = success_task.delay()

        assert successful.get(timeout=10) == "still working"


def test_concurrent_task_failures_are_isolated(flask_app):
    async_celery = flask_app.extensions["async_celery"]
    task = flask_app.extensions["concurrent_failure_task"]

    with start_worker(
        async_celery.celery,
        concurrency=5,
        pool="flask_async_celery.pool:AsyncIOPool",
        loglevel="INFO",
        perform_ping_check=False,
    ):
        results = [
            task.delay(i)
            for i in range(6)
        ]

        values = []

        for i, result in enumerate(results):
            if i % 2 == 0:
                with pytest.raises(ValueError, match=f"failed: {i}"):
                    result.get(timeout=10)
            else:
                values.append(result.get(timeout=10))

        assert values == [
            "success: 1",
            "success: 3",
            "success: 5",
        ]

        # Verify the executor is still usable.
        follow_up = task.delay(7)

        assert follow_up.get(timeout=10) == "success: 7"