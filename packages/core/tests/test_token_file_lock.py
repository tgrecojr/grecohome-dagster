"""Tests for the cross-process advisory token lock.

The headline test spawns a real child process so it actually exercises the
inter-process mutual exclusion the OAuth refresh depends on (an in-process
``asyncio.Lock`` can't cover Dagster's per-step subprocesses).
"""

import fcntl
import os
import subprocess
import sys
import time

import pytest

from grecohome_core.tokens.file_lock import InterProcessLock, interprocess_lock

# Child that grabs the lock, signals it holds it (creates ``held``), then keeps
# holding until the parent removes ``release`` -- so synchronization is by
# sentinel files, not by racing sleeps.
_WORKER = """
import os, sys, time
from grecohome_core.tokens.file_lock import interprocess_lock

lock_path, held_flag, release_flag = sys.argv[1:4]
with interprocess_lock(lock_path):
    open(held_flag, "w").close()
    while os.path.exists(release_flag):
        time.sleep(0.01)
"""


def _wait_for(path: str, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not os.path.exists(path):
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {path}")
        time.sleep(0.01)


def _held_by_other_process(lock_path: str) -> bool:
    """True if a non-blocking exclusive lock cannot be taken (someone holds it)."""
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    except BlockingIOError:
        return True
    finally:
        os.close(fd)


@pytest.mark.unit
class TestInterProcessLock:
    def test_excludes_another_process(self, tmp_path):
        lock_path = str(tmp_path / "token.json.lock")
        held_flag = str(tmp_path / "held")
        release_flag = str(tmp_path / "release")
        script = tmp_path / "worker.py"
        script.write_text(_WORKER)
        open(release_flag, "w").close()  # child holds the lock while this exists

        child = subprocess.Popen(
            [sys.executable, str(script), lock_path, held_flag, release_flag]
        )
        try:
            _wait_for(held_flag)  # child now holds the lock
            assert _held_by_other_process(lock_path) is True
            os.remove(release_flag)  # tell the child to release + exit
            assert child.wait(timeout=10) == 0
            # Lock is free again once the holder exited.
            assert _held_by_other_process(lock_path) is False
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)

    def test_context_manager_creates_lock_file_and_parents(self, tmp_path):
        lock_path = tmp_path / "nested" / "deep" / "token.json.lock"
        with interprocess_lock(str(lock_path)):
            assert lock_path.exists()

    def test_released_after_with_block(self, tmp_path):
        lock_path = str(tmp_path / "token.json.lock")
        with interprocess_lock(lock_path):
            pass
        # A fresh non-blocking acquire succeeds -> the lock was released.
        assert _held_by_other_process(lock_path) is False

    def test_acquire_is_idempotent_while_held(self, tmp_path):
        lock = InterProcessLock(str(tmp_path / "token.json.lock"))
        lock.acquire()
        lock.acquire()  # no-op, must not deadlock or open a second fd
        lock.release()
        lock.release()  # safe to release when not held
