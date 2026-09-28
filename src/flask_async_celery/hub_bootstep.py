from __future__ import annotations

import logging
from functools import wraps
from typing import Any, Callable

from celery import bootsteps

from .hub_wakeup import HubWakeup

logger = logging.getLogger(__name__)


class AsyncIOHubWakeupBootStep(bootsteps.StartStopStep):
    """
    Install cross-thread wakeups on the Celery Kombu Hub.

    Kombu's Hub.call_soon() is thread-safe when adding callbacks to
    the ready queue, but it does not wake a Hub blocked in poll().
    """

    requires = {
        "celery.worker.components:Hub",
        "celery.worker.components:Pool",
    }

    def __init__(self, worker, **kwargs: Any) -> None:
        self.worker = worker

        self.wakeup: HubWakeup | None = None

        self._original_call_soon: Callable[..., Any] | None = None
        self._original_reset: Callable[..., Any] | None = None

        self._installed = False

        super().__init__(worker, **kwargs)

    def start(self, worker) -> None:
        hub = getattr(worker, "hub", None)
        pool = getattr(worker, "pool", None)

        if hub is None:
            logger.warning(
                "AsyncIOHubWakeupBootStep: worker has no hub"
            )
            return

        if pool is None:
            logger.warning(
                "AsyncIOHubWakeupBootStep: worker has no pool"
            )
            return

        if self._installed:
            logger.warning(
                "AsyncIOHubWakeupBootStep: already installed"
            )
            return

        wakeup = HubWakeup(hub)
        wakeup.register()

        original_call_soon = hub.call_soon
        original_reset = hub.reset

        @wraps(original_call_soon)
        def call_soon(callback, *args):
            result = original_call_soon(callback, *args)
            wakeup.wake()
            return result

        @wraps(original_reset)
        def reset(*args, **kwargs):
            result = original_reset(*args, **kwargs)

            # Hub.reset() replaces the poller/registrations.
            if not wakeup._closed:
                wakeup.re_register()

            return result

        hub.call_soon = call_soon
        hub.reset = reset

        self.wakeup = wakeup
        self._original_call_soon = original_call_soon
        self._original_reset = original_reset

        worker.asyncio_hub_wakeup = wakeup
        pool.asyncio_hub_wakeup = wakeup

        self._installed = True

        logger.info("AsyncIO Hub wakeup installed")

    def stop(self, worker) -> None:
        self._uninstall(worker)

    def terminate(self, worker) -> None:
        self._uninstall(worker)

    def _uninstall(self, worker) -> None:
        if not self._installed:
            return

        hub = getattr(worker, "hub", None)
        wakeup = self.wakeup

        if wakeup is not None:
            wakeup.close()

        if hub is not None:
            if self._original_call_soon is not None:
                hub.call_soon = self._original_call_soon

            if self._original_reset is not None:
                hub.reset = self._original_reset

        worker.asyncio_hub_wakeup = None

        pool = getattr(worker, "pool", None)
        if pool is not None:
            pool.asyncio_hub_wakeup = None

        self.wakeup = None
        self._original_call_soon = None
        self._original_reset = None
        self._installed = False

        logger.info("AsyncIO Hub wakeup uninstalled")