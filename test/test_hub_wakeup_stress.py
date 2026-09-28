from __future__ import annotations

import threading
import time

from kombu.asynchronous import Hub

from flask_async_celery.hub_wakeup import HubWakeup


def test_hub_wakeup_handles_many_cross_thread_wakeups():
    hub = Hub()
    wakeup = HubWakeup(hub)

    wakeup.register()

    try:
        completed = threading.Event()
        counter = {"value": 0}

        def callback() -> None:
            counter["value"] += 1

            if counter["value"] == 100:
                completed.set()

        loop = hub.create_loop()

        def producer() -> None:
            for _ in range(100):
                hub.call_soon(callback)
                wakeup.wake()

        thread = threading.Thread(target=producer)

        thread.start()

        deadline = time.monotonic() + 3

        while not completed.is_set():
            if time.monotonic() >= deadline:
                break

            next(loop)

        thread.join(timeout=3)

        assert not thread.is_alive()
        assert completed.is_set()
        assert counter["value"] == 100

    finally:
        wakeup.close()
        hub.close()
