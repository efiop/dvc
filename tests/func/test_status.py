import os

from mock import patch

from dvc.main import main
from dvc.compat import fspath
from tests.basic_env import TestDvc


class TestStatus(TestDvc):
    def test_quiet(self):
        self.dvc.add(self.FOO)

        ret = main(["status", "--quiet"])
        self.assertEqual(ret, 0)

        os.remove(self.FOO)
        os.rename(self.BAR, self.FOO)

        ret = main(["status", "--quiet"])
        self.assertEqual(ret, 1)

    @patch("dvc.repo.status._cloud_status", return_value=True)
    def test_implied_cloud(self, mock_status):
        main(["status", "--remote", "something"])
        mock_status.assert_called()


def test_status_non_dvc_repo_import(tmp_dir, dvc, git_dir):
    with git_dir.branch("branch", new=True):
        git_dir.scm_gen("file", "first version", commit="first version")

    dvc.imp(fspath(git_dir), "file", "file", rev="branch")

    assert dvc.status(["file.dvc"]) == {}

    with git_dir.branch("branch", new=False):
        git_dir.scm_gen("file", "second version", commit="update file")

    (status,) = dvc.status(["file.dvc"])["file.dvc"]
    assert status == {
        "changed deps": {"file ({})".format(git_dir): "update available"}
    }


def test_status_before_and_after_dvc_init(tmp_dir, dvc, git_dir):
    git_dir.scm_gen("file", "first version", commit="first verison")
    old_rev = git_dir.scm.get_rev()

    dvc.imp(fspath(git_dir), "file", "file")

    assert dvc.status(["file.dvc"]) == {}

    with git_dir.chdir():
        git_dir.init(dvc=True)
        git_dir.scm.repo.index.remove(["file"])
        os.remove("file")
        git_dir.dvc_gen("file", "second version", commit="with dvc")
        new_rev = git_dir.scm.get_rev()

    assert old_rev != new_rev

    (status,) = dvc.status(["file.dvc"])["file.dvc"]
    assert status == {
        "changed deps": {
            "file ({})".format(fspath(git_dir)): "update available"
        }
    }


def test_status_on_pipeline_stages(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar")

    stage.cmd = "  ".join(stage.cmd.split())
    stage.dvcfile._dump_pipeline_file(stage)
    assert dvc.status() == {"dvc.yaml:copy-foo-bar": ["changed command"]}

    # delete outputs
    (tmp_dir / "bar").unlink()
    assert dvc.status() == {
        "dvc.yaml:copy-foo-bar": [
            {"changed outs": {"bar": "deleted"}},
            "changed command",
        ]
    }
    (tmp_dir / "foo").unlink()
    assert dvc.status() == {
        "foo.dvc": [{"changed outs": {"foo": "deleted"}}],
        "dvc.yaml:copy-foo-bar": [
            {"changed deps": {"foo": "deleted"}},
            {"changed outs": {"bar": "deleted"}},
            "changed command",
        ],
    }
