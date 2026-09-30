import asyncio

from flask import Flask

from flask_async_celery import AsyncCelery

app = Flask(__name__)

celery = AsyncCelery(
    app,
    broker_url="redis://127.0.0.1:6379/0",
    result_backend="redis://127.0.0.1:6379/1",
    max_tasks=5,
)


@celery.task
async def slow_task(seconds=30):
    await asyncio.sleep(seconds)

    return {
        "status": "completed",
        "seconds": seconds,
    }


@celery.task
async def successful_task():
    await asyncio.sleep(1)
    return "success"


@celery.task
async def failing_task():
    await asyncio.sleep(1)
    raise ValueError("intentional test failure")

celery_app = celery.celery