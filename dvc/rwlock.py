import os
import json

from contextlib import contextmanager

from voluptuous import Schema, Optional

from .lock import LockError
from .utils.fs import relpath
from .utils.compat import convert_to_unicode


SCHEMA = {Optional(str): {Optional("writer"): int, Optional("readers"): [int]}}
COMPLIED_SCHEMA = Schema(SCHEMA)


@contextmanager
def _rwlock(lock_dir):
    path = os.path.join(lock_dir, "rwlock")
    try:
        with open(path, "r") as fobj:
            rwlock = json.load(fobj)
    except (FileNotFoundError, json.decoder.JSONDecodeError):
        rwlock = {}
    rwlock = COMPLIED_SCHEMA(convert_to_unicode(rwlock))
    yield rwlock
    with open(path, "w+") as fobj:
        json.dump(rwlock, fobj)


def _check_no_writer(lock, path):
    writer = lock[path].get("writer")
    if writer:
        raise LockError(
            "'{}' is busy, it is being written to by '{}'.".format(
                relpath(path), writer
            )
        )


def _check_no_readers(lock, path):
    readers = lock[path].get("readers")
    if readers:
        raise LockError(
            "'{}' is busy, it is being read by '{}'".format(
                relpath(path), str(readers)
            )
        )


def _acquire_read(lock, paths):
    for path in paths:
        if path in lock:
            _check_no_writer(lock, path)
        else:
            lock[path] = {"readers": []}
        lock[path]["readers"].append(os.getpid())


def _acquire_write(lock, paths):
    for path in paths:
        if path in lock:
            _check_no_writer(lock, path)
            _check_no_readers(lock, path)
        lock[path] = {"writer": os.getpid()}


def _release_read(lock, paths):
    for path in paths:
        if path not in lock:
            continue
        readers = lock[path]["readers"]
        if os.getpid() in readers:
            readers.remove(os.getpid())
        if not readers:
            del lock[path]["readers"]
        if not lock[path]:
            del lock[path]


def _release_write(lock, paths):
    for path in paths:
        if path not in lock:
            continue
        writer = lock[path]["writer"]
        if writer == os.getpid():
            del lock[path]["writer"]
        if not lock[path]:
            del lock[path]


@contextmanager
def rwlock(tmp_dir, read, write):
    with _rwlock(tmp_dir) as lock:
        _acquire_read(lock, read)
        _acquire_write(lock, write)

    try:
        yield
    finally:
        with _rwlock(tmp_dir) as lock:
            _release_write(lock, write)
            _release_read(lock, read)
