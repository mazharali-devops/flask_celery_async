from __future__ import annotations

from celery.worker.control import Panel


@Panel.register
def async_stats(state):
    """Return AsyncIO executor statistics."""
    executor = getattr(state.worker, "async_executor", None)

    if executor is None:
        return {
            "asyncio-enabled": False,
        }

    return {
        "asyncio-enabled": True,
        **executor.stats(),
    }


@Panel.register
def async_tasks(state):
    """Return currently running AsyncIO tasks."""
    executor = getattr(state.worker, "async_executor", None)

    if executor is None:
        return {
            "asyncio-enabled": False,
            "tasks": [],
        }

    return {
        "asyncio-enabled": True,
        "tasks": executor.running_tasks(),
    }