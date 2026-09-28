from __future__ import annotations

import threading
import time

from kombu.asynchronous import Hub

from flask_async_celery.hub_wakeup import HubWakeup


def test_hub_wakeup_survives_reset():
    hub = Hub()
    wakeup = HubWakeup(hub)

    original_call_soon = hub.call_soon
    original_reset = hub.reset

    wakeup.register()

    def call_soon(callback, *args):
        result = original_call_soon(callback, *args)
        wakeup.wake()
        return result

    def reset(*args, **kwargs):
        result = original_reset(*args, **kwargs)

        if not wakeup._closed:
            wakeup.register()

        return result

    hub.call_soon = call_soon
    hub.reset = reset

    try:
        # --------------------------------------------------
        # Phase 1: normal Hub operation
        # --------------------------------------------------

        phase1 = threading.Event()

        hub.call_soon(phase1.set)

        loop = hub.create_loop()

        started = time.monotonic()

        next(loop)

        elapsed = time.monotonic() - started

        assert phase1.is_set()
        assert elapsed < 1.0

        # --------------------------------------------------
        # Reset Hub
        # --------------------------------------------------

        hub.reset()

        assert wakeup._registered is True

        # Celery creates a NEW loop after a reset/reconnect.
        loop = hub.create_loop()

        # --------------------------------------------------
        # Phase 2: operation after reset
        # --------------------------------------------------

        phase2 = threading.Event()

        hub.call_soon(phase2.set)

        started = time.monotonic()

        next(loop)

        elapsed = time.monotonic() - started

        assert phase2.is_set()
        assert elapsed < 1.0

    finally:
        wakeup.close()
        hub.close()
