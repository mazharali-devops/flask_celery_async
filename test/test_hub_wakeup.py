from __future__ import annotations

import os
import threading
import time

from kombu.asynchronous import Hub

from flask_async_celery.hub_wakeup import HubWakeup


def test_hub_wakeup_wakes_blocked_hub():
    hub = Hub()
    wakeup = HubWakeup(hub)

    wakeup.register()

    callback_called = threading.Event()

    def callback() -> None:
        callback_called.set()

    hub.call_soon(callback)

    loop = hub.create_loop()

    def wake_from_thread() -> None:
        time.sleep(0.2)
        wakeup.wake()

    thread = threading.Thread(target=wake_from_thread)
    thread.start()

    started = time.monotonic()

    try:
        next(loop)

        elapsed = time.monotonic() - started

        assert callback_called.is_set()
        assert elapsed < 1.0

    finally:
        thread.join(timeout=2)
        wakeup.close()
        hub.close()


def test_hub_wakeup_coalesces_concurrent_wakes(monkeypatch):
    hub = Hub()
    wakeup = HubWakeup(hub)

    wakeup.register()

    writes = 0
    writes_lock = threading.Lock()

    original_write = os.write

    def counted_write(fd, data):
        nonlocal writes

        with writes_lock:
            writes += 1

        return original_write(fd, data)

    monkeypatch.setattr(
        "flask_async_celery.hub_wakeup.os.write",
        counted_write,
    )

    try:
        with wakeup._lock:
            wakeup._wake_pending = True

        wakeup.wake()

        assert writes == 0

        with wakeup._lock:
            wakeup._wake_pending = False

        wakeup.wake()

        assert writes == 1

    finally:
        wakeup.close()
        hub.close()


def test_hub_wakeup_can_unregister_and_register():
    hub = Hub()
    wakeup = HubWakeup(hub)

    try:
        wakeup.register()

        assert wakeup._registered is True

        wakeup.unregister()

        assert wakeup._registered is False

        wakeup.register()

        assert wakeup._registered is True

    finally:
        wakeup.close()
        hub.close()
