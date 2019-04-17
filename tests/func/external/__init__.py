from __future__ import unicode_literals
from dvc.utils.compat import str, urljoin

import os
import shutil
import filecmp
import getpass
import posixpath
from subprocess import Popen, PIPE

import boto3
import uuid
import paramiko
from google.cloud import storage as gc
from flaky.flaky_decorator import flaky

from dvc.main import main
from dvc.repo import Repo as DvcRepo

from tests.basic_env import TestDvc
from tests.func.test_data_cloud import _should_test_aws, TEST_AWS_REPO_BUCKET
from tests.func.test_data_cloud import _should_test_gcp, TEST_GCP_REPO_BUCKET
from tests.func.test_data_cloud import _should_test_ssh, _should_test_hdfs
from tests.utils.httpd import StaticFileServer
from mock import patch


class TestReproExternalBase(TestDvc):
    def should_test(self):
        return False

    @property
    def cache_scheme(self):
        return self.scheme

    @property
    def scheme(self):
        return None

    @property
    def scheme_sep(self):
        return "://"

    @property
    def sep(self):
        return "/"

    def check_already_cached(self, stage):
        stage.outs[0].remove()

        patch_download = patch.object(
            stage.deps[0], "download", wraps=stage.deps[0].download
        )

        patch_checkout = patch.object(
            stage.outs[0], "checkout", wraps=stage.outs[0].checkout
        )

        patch_run = patch.object(stage, "_run", wraps=stage._run)

        with self.dvc.state:
            with patch_download as mock_download:
                with patch_checkout as mock_checkout:
                    with patch_run as mock_run:
                        stage.run()

                        mock_run.assert_not_called()
                        mock_download.assert_not_called()
                        mock_checkout.assert_called_once()

    @patch("dvc.prompt.confirm", return_value=True)
    def test(self, mock_prompt):
        if not self.should_test():
            return

        cache = (
            self.scheme
            + self.scheme_sep
            + self.bucket
            + self.sep
            + str(uuid.uuid4())
        )

        ret = main(["config", "cache." + self.cache_scheme, "myrepo"])
        self.assertEqual(ret, 0)
        ret = main(["remote", "add", "myrepo", cache])
        self.assertEqual(ret, 0)
        ret = main(["remote", "modify", "myrepo", "type", "hardlink"])
        self.assertEqual(ret, 0)

        remote_name = "myremote"
        remote_key = str(uuid.uuid4())
        remote = (
            self.scheme + self.scheme_sep + self.bucket + self.sep + remote_key
        )

        ret = main(["remote", "add", remote_name, remote])
        self.assertEqual(ret, 0)
        ret = main(["remote", "modify", remote_name, "type", "hardlink"])
        self.assertEqual(ret, 0)

        self.dvc = DvcRepo(".")

        foo_key = remote_key + self.sep + self.FOO
        bar_key = remote_key + self.sep + self.BAR

        foo_path = (
            self.scheme + self.scheme_sep + self.bucket + self.sep + foo_key
        )
        bar_path = (
            self.scheme + self.scheme_sep + self.bucket + self.sep + bar_key
        )

        # Using both plain and remote notation
        out_foo_path = "remote://" + remote_name + "/" + self.FOO
        out_bar_path = bar_path

        self.write(self.bucket, foo_key, self.FOO_CONTENTS)

        import_stage = self.dvc.imp(out_foo_path, "import")

        self.assertTrue(os.path.exists("import"))
        self.assertTrue(filecmp.cmp("import", self.FOO, shallow=False))
        self.assertEqual(self.dvc.status(import_stage.path), {})
        self.check_already_cached(import_stage)

        import_remote_stage = self.dvc.imp(
            out_foo_path, out_foo_path + "_imported"
        )
        self.assertEqual(self.dvc.status(import_remote_stage.path), {})

        cmd_stage = self.dvc.run(
            outs=[out_bar_path],
            deps=[out_foo_path],
            cmd=self.cmd(foo_path, bar_path),
        )

        self.assertEqual(self.dvc.status(cmd_stage.path), {})
        self.assertEqual(self.dvc.status(), {})
        self.check_already_cached(cmd_stage)

        self.write(self.bucket, foo_key, self.BAR_CONTENTS)

        self.assertNotEqual(self.dvc.status(), {})

        stages = self.dvc.reproduce(import_stage.path)
        self.assertEqual(len(stages), 1)
        self.assertTrue(os.path.exists("import"))
        self.assertTrue(filecmp.cmp("import", self.BAR, shallow=False))
        self.assertEqual(self.dvc.status(import_stage.path), {})

        stages = self.dvc.reproduce(import_remote_stage.path)
        self.assertEqual(len(stages), 1)
        self.assertEqual(self.dvc.status(import_remote_stage.path), {})

        stages = self.dvc.reproduce(cmd_stage.path)
        self.assertEqual(len(stages), 1)
        self.assertEqual(self.dvc.status(cmd_stage.path), {})

        self.assertEqual(self.dvc.status(), {})
        self.dvc.gc()
        self.assertEqual(self.dvc.status(), {})

        self.dvc.remove(cmd_stage.path, outs_only=True)
        self.assertNotEqual(self.dvc.status(cmd_stage.path), {})

        self.dvc.checkout(cmd_stage.path, force=True)
        self.assertEqual(self.dvc.status(cmd_stage.path), {})

        remote_key = str(uuid.uuid4())
        remote = (
            self.scheme + self.scheme_sep + self.bucket + self.sep + remote_key
        )
        self.write(
            self.bucket, remote_key + self.sep + "foo", self.FOO_CONTENTS
        )
        self.write(
            self.bucket,
            remote_key + self.sep + "subdir" + self.sep + "bar",
            self.BAR_CONTENTS,
        )

        self.dvc.add(remote)


class TestReproExternalS3(TestReproExternalBase):
    def should_test(self):
        return _should_test_aws()

    @property
    def scheme(self):
        return "s3"

    @property
    def bucket(self):
        return TEST_AWS_REPO_BUCKET

    def cmd(self, i, o):
        return "aws s3 cp {} {}".format(i, o)

    def write(self, bucket, key, body):
        s3 = boto3.resource("s3")
        s3.Bucket(bucket).put_object(Key=key, Body=body)


class TestReproExternalGS(TestReproExternalBase):
    def should_test(self):
        return _should_test_gcp()

    @property
    def scheme(self):
        return "gs"

    @property
    def bucket(self):
        return TEST_GCP_REPO_BUCKET

    def cmd(self, i, o):
        return "gsutil cp {} {}".format(i, o)

    def write(self, bucket, key, body):
        client = gc.Client()
        bucket = client.bucket(bucket)
        bucket.blob(key).upload_from_string(body)


class TestReproExternalHDFS(TestReproExternalBase):
    def should_test(self):
        return _should_test_hdfs()

    @property
    def scheme(self):
        return "hdfs"

    @property
    def bucket(self):
        return "{}@127.0.0.1".format(getpass.getuser())

    def cmd(self, i, o):
        return "hadoop fs -cp {} {}".format(i, o)

    def write(self, bucket, key, body):
        url = self.scheme + "://" + bucket + "/" + key
        p = Popen(
            "hadoop fs -rm -f {}".format(url),
            shell=True,
            executable=os.getenv("SHELL"),
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
        )
        p.communicate()

        p = Popen(
            "hadoop fs -mkdir -p {}".format(posixpath.dirname(url)),
            shell=True,
            executable=os.getenv("SHELL"),
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
        )
        out, err = p.communicate()
        if p.returncode != 0:
            print(out)
            print(err)
        self.assertEqual(p.returncode, 0)

        with open("tmp", "w+") as fd:
            fd.write(body)

        p = Popen(
            "hadoop fs -copyFromLocal {} {}".format("tmp", url),
            shell=True,
            executable=os.getenv("SHELL"),
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
        )
        out, err = p.communicate()
        if p.returncode != 0:
            print(out)
            print(err)
        self.assertEqual(p.returncode, 0)


@flaky(max_runs=3, min_passes=1)
class TestReproExternalSSH(TestReproExternalBase):
    _dir = None

    def should_test(self):
        return _should_test_ssh()

    @property
    def scheme(self):
        return "ssh"

    @property
    def bucket(self):
        if not self._dir:
            self._dir = TestDvc.mkdtemp()
        return "{}@127.0.0.1:{}".format(getpass.getuser(), self._dir)

    def cmd(self, i, o):
        i = i.strip("ssh://")
        o = o.strip("ssh://")
        return "scp {} {}".format(i, o)

    def write(self, bucket, key, body):
        path = posixpath.join(self._dir, key)

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect("127.0.0.1")

        sftp = ssh.open_sftp()
        try:
            sftp.stat(path)
            sftp.remove(path)
        except IOError:
            pass

        stdin, stdout, stderr = ssh.exec_command(
            "mkdir -p $(dirname {})".format(path)
        )
        self.assertEqual(stdout.channel.recv_exit_status(), 0)

        with sftp.open(path, "w+") as fobj:
            fobj.write(body)


class TestReproExternalLOCAL(TestReproExternalBase):
    def setUp(self):
        super(TestReproExternalLOCAL, self).setUp()
        self.tmpdir = TestDvc.mkdtemp()
        ret = main(["config", "cache.type", "hardlink"])
        self.assertEqual(ret, 0)
        self.dvc = DvcRepo(".")

    def should_test(self):
        return True

    @property
    def cache_scheme(self):
        return "local"

    @property
    def scheme(self):
        return ""

    @property
    def scheme_sep(self):
        return ""

    @property
    def sep(self):
        return os.sep

    @property
    def bucket(self):
        return self.tmpdir

    def cmd(self, i, o):
        if os.name == "nt":
            return "copy {} {}".format(i, o)
        return "cp {} {}".format(i, o)

    def write(self, bucket, key, body):
        path = os.path.join(bucket, key)
        dname = os.path.dirname(path)

        if not os.path.exists(dname):
            os.makedirs(dname)

        with open(path, "w+") as fd:
            fd.write(body)


class TestReproExternalHTTP(TestReproExternalBase):
    _external_cache_id = None

    @property
    def remote(self):
        return "http://localhost:8000/"

    @property
    def local_cache(self):
        return os.path.join(self.dvc.dvc_dir, "cache")

    @property
    def external_cache_id(self):
        if not self._external_cache_id:
            self._external_cache_id = str(uuid.uuid4())

        return self._external_cache_id

    @property
    def external_cache(self):
        return urljoin(self.remote, self.external_cache_id)

    def test(self):
        ret1 = main(["remote", "add", "mycache", self.external_cache])
        ret2 = main(["remote", "add", "myremote", self.remote])
        self.assertEqual(ret1, 0)
        self.assertEqual(ret2, 0)

        self.dvc = DvcRepo(".")

        # Import
        with StaticFileServer():
            import_url = urljoin(self.remote, self.FOO)
            import_output = "imported_file"
            import_stage = self.dvc.imp(import_url, import_output)

        self.assertTrue(os.path.exists(import_output))
        self.assertTrue(filecmp.cmp(import_output, self.FOO, shallow=False))

        self.dvc.remove("imported_file.dvc")

        with StaticFileServer(handler="Content-MD5"):
            import_url = urljoin(self.remote, self.FOO)
            import_output = "imported_file"
            import_stage = self.dvc.imp(import_url, import_output)

        self.assertTrue(os.path.exists(import_output))
        self.assertTrue(filecmp.cmp(import_output, self.FOO, shallow=False))

        # Run --deps
        with StaticFileServer():
            run_dependency = urljoin(self.remote, self.BAR)
            run_output = "remote_file"
            cmd = 'open("{}", "w+")'.format(run_output)

            with open("create-output.py", "w") as fd:
                fd.write(cmd)

            run_stage = self.dvc.run(
                deps=[run_dependency],
                outs=[run_output],
                cmd="python create-output.py",
            )
            self.assertTrue(run_stage is not None)

        self.assertTrue(os.path.exists(run_output))

        # Pull
        self.dvc.remove(import_stage.path, outs_only=True)
        self.assertFalse(os.path.exists(import_output))

        shutil.move(self.local_cache, self.external_cache_id)
        self.assertFalse(os.path.exists(self.local_cache))

        with StaticFileServer():
            self.dvc.pull(import_stage.path, remote="mycache")

        self.assertTrue(os.path.exists(import_output))
