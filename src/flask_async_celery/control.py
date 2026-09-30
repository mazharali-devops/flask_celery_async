from __future__ import annotations

from celery.worker.control import inspect_command


@inspect_command()
def async_stats(state, **kwargs):
    """Return AsyncIO executor statistics."""
    pool = state.consumer.pool
    executor = getattr(pool, "async_executor", None)

    if executor is None:
        return {
            "asyncio-enabled": False,
        }

    return {
        "asyncio-enabled": True,
        **executor.stats(),
    }


@inspect_command()
def async_tasks(state, **kwargs):
    """Return currently running AsyncIO tasks."""
    pool = state.consumer.pool
    executor = getattr(pool, "async_executor", None)

    if executor is None:
        return {
            "asyncio-enabled": False,
            "tasks": [],
        }

    return {
        "asyncio-enabled": True,
        "tasks": executor.running_tasks(),
    }