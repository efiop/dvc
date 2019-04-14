import mock
from unittest import TestCase

from dvc.remote.base import RemoteBase, RemoteCmdError, RemoteMissingDepsError


class TestRemoteBase(object):
    REMOTE_CLS = RemoteBase


class TestMissingDeps(TestCase, TestRemoteBase):
    def test(self):
        REQUIRES = {"foo": None, "bar": None, "mock": mock}
        with mock.patch.object(self.REMOTE_CLS, "REQUIRES", REQUIRES):
            with self.assertRaises(RemoteMissingDepsError):
                self.REMOTE_CLS(None, {})


class TestCmdError(TestCase, TestRemoteBase):
    def test(self):
        repo = None
        config = {}

        cmd = "sed 'hello'"
        ret = "1"
        err = "sed: expression #1, char 2: extra characters after command"

        with mock.patch.object(
            self.REMOTE_CLS,
            "remove",
            side_effect=RemoteCmdError("base", cmd, ret, err),
        ):
            with self.assertRaises(RemoteCmdError):
                self.REMOTE_CLS(repo, config).remove("file")


# class TestCacheExists(TestCase):
#    def test(self):
#        config = {
#            "url": "base://example/prefix",
#            "connection_string": "1234567",
#        }
#        remote = RemoteBase(None, config)
#
#        with mock.patch.object(remote, "changed_cache", return_value=True):
#            with mock.patch.object(remote, "isdir", return_value=False):
#                with mock.patch.object(remote, "copy") as cp:
#                    remote.save(
#                        {"scheme": None, "path": "example"},
#                        {remote.PARAM_CHECKSUM: "1234567890"},
#                    )
#                    cp.assert_called_once()
#
#        with mock.patch.object(remote, "changed_cache", return_value=False):
#            with mock.patch.object(remote, "isdir", return_value=False):
#                with mock.patch.object(remote, "copy") as cp:
#                    remote.save(
#                        {"path": "example"},
#                        {remote.PARAM_CHECKSUM: "1234567890"},
#                    )
#                    cp.assert_not_called()
