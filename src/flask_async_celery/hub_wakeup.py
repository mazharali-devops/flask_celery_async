from __future__ import annotations

import logging
import os
import threading
from typing import Any

from kombu.asynchronous import READ

logger = logging.getLogger(__name__)


class HubWakeup:
    """
    Wake a Kombu Hub when call_soon() is invoked from another thread.

    Kombu's Hub.call_soon() is thread-safe for adding callbacks to
    the ready queue, but it does not wake a Hub blocked in poll().

    This helper uses a non-blocking pipe registered with the Hub's
    poller to provide an explicit cross-thread wakeup mechanism.
    """

    def __init__(self, hub: Any) -> None:
        self.hub = hub

        self.read_fd, self.write_fd = os.pipe()

        os.set_blocking(self.read_fd, False)
        os.set_blocking(self.write_fd, False)

        self._lock = threading.Lock()
        self._closed = False
        self._registered = False
        self._wake_pending = False

    def register(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("HubWakeup is closed")

            if self._registered:
                return

            self._registered = True

        try:
            self.hub.add(
                self.read_fd,
                self._on_wakeup,
                READ,
            )
        except BaseException:
            with self._lock:
                self._registered = False
            raise

        logger.debug(
            "Hub wakeup registered: fd=%s",
            self.read_fd,
        )

    def wake(self) -> None:
        with self._lock:
            if self._closed:
                return

            if self._wake_pending:
                return

            self._wake_pending = True

        try:
            os.write(self.write_fd, b"x")
        except BlockingIOError:
            # The pipe is already readable.
            # The existing wakeup will be handled by the Hub.
            return
        except OSError:
            with self._lock:
                self._wake_pending = False

            logger.debug(
                "Hub wakeup pipe write failed",
                exc_info=True,
            )

    def _on_wakeup(self) -> None:
        while True:
            try:
                data = os.read(self.read_fd, 4096)

                if not data:
                    break

            except BlockingIOError:
                break

            except OSError:
                logger.debug(
                    "Hub wakeup pipe read failed",
                    exc_info=True,
                )
                break

        with self._lock:
            self._wake_pending = False

    def re_register(self) -> None:
        with self._lock:
            if self._closed:
                return

            self._registered = False

        self.register()

    def unregister(self) -> None:
        with self._lock:
            if not self._registered:
                return

            self._registered = False

        try:
            self.hub.remove(self.read_fd)
        except Exception:
            logger.debug(
                "Failed to unregister Hub wakeup FD",
                exc_info=True,
            )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return

            self._closed = True
            self._registered = False

        try:
            self.hub.remove(self.read_fd)
        except Exception:
            logger.debug(
                "Failed to remove Hub wakeup FD during close",
                exc_info=True,
            )

        for fd in (self.read_fd, self.write_fd):
            try:
                os.close(fd)
            except OSError:
                pass

        logger.debug("Hub wakeup closed")

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed