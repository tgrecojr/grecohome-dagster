"""Cross-process advisory file lock (``fcntl.flock``).

Serializes a critical section across processes on one host. The motivating use is
the OAuth token read->refresh->write in :mod:`grecohome_whoop.auth.token_manager`:
Whoop rotates the refresh token on every refresh and treats a *reused* (already
consumed) refresh token as a replay attack, revoking the whole grant. Dagster runs
each step in its own subprocess and the bronze/snapshots jobs are separate runs, so
an in-process ``asyncio.Lock`` cannot stop two of them from refreshing at once and
double-spending the single-use token. A host-wide file lock can.

The lock is advisory and tied to the open file description, so the kernel releases
it automatically when the fd is closed or the process dies -- a crash never wedges
it. All participants must share one filesystem for the lock path (true here: a
single host with one mounted token dir). The lock file is a small, empty sentinel
next to the token file; it is created on demand and never unlinked (removing it
would let a new waiter lock a different inode than the current holder).
"""

import fcntl
import os
from collections.abc import Iterator
from contextlib import contextmanager


class InterProcessLock:
    """An exclusive advisory lock over a sentinel file, held across processes.

    Acquisition blocks until the lock is free. ``acquire`` is a blocking syscall;
    call it from a worker thread (e.g. ``asyncio.to_thread``) when you must not
    block an event loop. ``release`` is non-blocking.
    """

    def __init__(self, lock_path: str) -> None:
        self.lock_path = lock_path
        self._fd: int | None = None

    def acquire(self) -> None:
        """Block until the exclusive lock is held (a no-op while already held)."""
        if self._fd is not None:
            return
        directory = os.path.dirname(self.lock_path) or "."
        os.makedirs(directory, exist_ok=True)
        fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    def release(self) -> None:
        """Release the lock and close the descriptor (safe if not held)."""
        fd, self._fd = self._fd, None
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def __enter__(self) -> InterProcessLock:
        self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


@contextmanager
def interprocess_lock(lock_path: str) -> Iterator[None]:
    """Hold :class:`InterProcessLock` on ``lock_path`` for the with-block."""
    lock = InterProcessLock(lock_path)
    lock.acquire()
    try:
        yield
    finally:
        lock.release()
