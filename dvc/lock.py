"""Manages dvc lock file."""
from __future__ import unicode_literals

import time

import zc.lockfile

from dvc.exceptions import DvcException


DEFAULT_TIMEOUT = 5

FAILED_TO_LOCK_MESSAGE = (
    "cannot perform the command because another DVC process seems to be "
    "running on this project. If that is not the case, manually remove "
    "`.dvc/lock` and try again."
)


class LockError(DvcException):
    """Thrown when unable to acquire the lock for dvc repo."""


class Lock(object):
    """Class for dvc repo lock.

    Uses zc.lockfile as backend.
    """

    def __init__(self, lockfile, tmp_dir=None):
        self.lockfile = lockfile
        self._lock = None

    @property
    def files(self):
        return [self.lockfile]

    def _do_lock(self):
        try:
            self._lock = zc.lockfile.LockFile(self.lockfile)
        except zc.lockfile.LockError:
            raise LockError(FAILED_TO_LOCK_MESSAGE)

    def lock(self):
        try:
            self._do_lock()
            return
        except LockError:
            time.sleep(DEFAULT_TIMEOUT)

        self._do_lock()

    def unlock(self):
        self._lock.close()
        self._lock = None

    def __enter__(self):
        self.lock()

    def __exit__(self, typ, value, tbck):
        self.unlock()
