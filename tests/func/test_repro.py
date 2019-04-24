from __future__ import unicode_literals
from dvc.utils.compat import str, Path

import os
import re
import shutil
import filecmp

import pytest

from dvc.main import main
from dvc.repo import Repo as DvcRepo
from dvc.utils import file_checksum
from dvc.utils.stage import load_stage_file, dump_stage_file
from dvc.remote.local import RemoteLOCAL
from dvc.stage import Stage, StageFileDoesNotExistError
from dvc.system import System
from dvc.output.base import OutputBase
from dvc.exceptions import (
    CyclicGraphError,
    StagePathAsOutputError,
    ReproductionError,
)

from tests.basic_env import TestDvc
from mock import patch


class TestRepro(TestDvc):
    def setUp(self):
        super(TestRepro, self).setUp()

        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.foo_stage = stages[0]
        self.assertTrue(self.foo_stage is not None)

        self.file1 = "file1"
        self.file1_stage = self.file1 + ".dvc"
        self.dvc.run(
            fname=self.file1_stage,
            outs=[self.file1],
            deps=[self.FOO, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.FOO, self.file1),
        )


class TestReproFail(TestRepro):
    def test(self):
        os.unlink(self.CODE)

        ret = main(["repro", self.file1_stage])
        self.assertNotEqual(ret, 0)


class TestReproCyclicGraph(TestDvc):
    def test(self):
        self.dvc.run(
            deps=[self.FOO], outs=["bar.txt"], cmd="echo bar > bar.txt"
        )

        self.dvc.run(
            deps=["bar.txt"], outs=["baz.txt"], cmd="echo baz > baz.txt"
        )

        stage_dump = {
            "cmd": "echo baz > foo",
            "deps": [{"path": "baz.txt"}],
            "outs": [{"path": self.FOO}],
        }
        dump_stage_file("cycle.dvc", stage_dump)

        with self.assertRaises(CyclicGraphError):
            self.dvc.reproduce("cycle.dvc")


class TestReproWorkingDirectoryAsOutput(TestDvc):
    """
    |  stage.cwd  |  out.path | cwd as output |
    |:-----------:|:---------:|:-------------:|
    |     dir     |    dir    |      True     |
    | dir/subdir/ |    dir    |      True     |
    |     dir     |   dir-1   |     False     |
    |      .      | something |     False     |
    """

    def test(self):
        # File structure:
        #       .
        #       |-- dir1
        #       |  |__ dir2.dvc         (out.path == ../dir2)
        #       |__ dir2
        #           |__ something.dvc    (stage.cwd == ./dir2)

        os.mkdir(os.path.join(self.dvc.root_dir, "dir1"))

        self.dvc.run(
            cwd="dir1",
            outs=["../dir2"],
            cmd="mkdir {path}".format(path=os.path.join("..", "dir2")),
        )

        faulty_stage_path = os.path.join("dir2", "something.dvc")

        output = os.path.join("..", "something")
        stage_dump = {
            "cmd": "echo something > {}".format(output),
            "outs": [{"path": output}],
        }
        dump_stage_file(faulty_stage_path, stage_dump)

        with self.assertRaises(StagePathAsOutputError):
            self.dvc.reproduce(faulty_stage_path)

    def test_nested(self):
        from dvc.stage import Stage

        #
        #       .
        #       |-- a
        #       |  |__ nested
        #       |     |__ dir
        #       |       |__ error.dvc     (stage.cwd == 'a/nested/dir')
        #       |__ b
        #          |__ nested.dvc         (stage.out == 'a/nested')
        dir1 = "b"
        dir2 = "a"

        os.mkdir(dir1)
        os.mkdir(dir2)

        nested_dir = os.path.join(dir2, "nested")
        out_dir = os.path.relpath(nested_dir, dir1)

        nested_stage = self.dvc.run(
            cwd=dir1,  # b
            outs=[out_dir],  # ../a/nested
            cmd="mkdir {path}".format(path=out_dir),
        )

        os.mkdir(os.path.join(nested_dir, "dir"))

        error_stage_path = os.path.join(nested_dir, "dir", "error.dvc")

        output = os.path.join("..", "..", "something")
        stage_dump = {
            "cmd": "echo something > {}".format(output),
            "outs": [{"path": output}],
        }
        dump_stage_file(error_stage_path, stage_dump)

        # NOTE: os.walk() walks in a sorted order and we need dir2 subdirs to
        # be processed before dir1 to load error.dvc first.
        with patch.object(DvcRepo, "stages") as mock_stages:
            mock_stages.return_value = [
                nested_stage,
                Stage.load(self.dvc, error_stage_path),
            ]

            with self.assertRaises(StagePathAsOutputError):
                self.dvc.reproduce(error_stage_path)

    def test_similar_paths(self):
        # File structure:
        #
        #       .
        #       |-- something.dvc   (out.path == something)
        #       |-- something
        #       |__ something-1
        #          |-- a
        #          |__ a.dvc        (stage.cwd == something-1)

        self.dvc.run(outs=["something"], cmd="mkdir something")

        os.mkdir("something-1")

        stage = os.path.join("something-1", "a.dvc")

        stage_dump = {"cmd": "echo a > a", "outs": [{"path": "a"}]}
        dump_stage_file(stage, stage_dump)

        try:
            self.dvc.reproduce(stage)
        except StagePathAsOutputError:
            self.fail("should not raise StagePathAsOutputError")


class TestReproDepUnderDir(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        self.dir_stage = stages[0]
        self.assertTrue(self.dir_stage is not None)

        self.file1 = "file1"
        self.file1_stage = self.file1 + ".dvc"
        self.dvc.run(
            fname=self.file1_stage,
            outs=[self.file1],
            deps=[self.DATA, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.DATA, self.file1),
        )

        self.assertTrue(filecmp.cmp(self.file1, self.DATA, shallow=False))

        os.unlink(self.DATA)
        shutil.copyfile(self.FOO, self.DATA)

        stages = self.dvc.reproduce(self.file1_stage)
        self.assertEqual(len(stages), 2)
        self.assertTrue(filecmp.cmp(self.file1, self.FOO, shallow=False))


class TestReproDepDirWithOutputsUnderIt(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        stages = self.dvc.add(self.DATA_SUB)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        stage = self.dvc.run(fname="Dvcfile", deps=[self.DATA, self.DATA_SUB])
        self.assertTrue(stage is not None)

        file1 = "file1"
        file1_stage = file1 + ".dvc"
        stage = self.dvc.run(
            fname=file1_stage,
            deps=[self.DATA_DIR],
            outs=[file1],
            cmd="python {} {} {}".format(self.CODE, self.DATA, file1),
        )
        self.assertTrue(stage is not None)

        os.unlink(self.DATA)
        shutil.copyfile(self.FOO, self.DATA)

        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 2)


class TestReproNoDeps(TestRepro):
    def test(self):
        out = "out"
        code_file = "out.py"
        stage_file = "out.dvc"
        code = (
            'import uuid\nwith open("{}", "w+") as fd:\n'
            "\tfd.write(str(uuid.uuid4()))\n".format(out)
        )
        with open(code_file, "w+") as fd:
            fd.write(code)
        self.dvc.run(
            fname=stage_file, outs=[out], cmd="python {}".format(code_file)
        )

        stages = self.dvc.reproduce(stage_file)
        self.assertEqual(len(stages), 1)


class TestReproForce(TestRepro):
    def test(self):
        stages = self.dvc.reproduce(self.file1_stage, force=True)
        self.assertEqual(len(stages), 2)


class TestReproChangedCode(TestRepro):
    def test(self):
        self.swap_code()

        stages = self.dvc.reproduce(self.file1_stage)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))
        self.assertEqual(len(stages), 1)

    def swap_code(self):
        os.unlink(self.CODE)
        new_contents = self.CODE_CONTENTS
        new_contents += "\nshutil.copyfile('{}', " "sys.argv[2])\n".format(
            self.BAR
        )
        self.create(self.CODE, new_contents)


class TestReproChangedData(TestRepro):
    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self.file1_stage)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))
        self.assertEqual(len(stages), 2)

    def swap_foo_with_bar(self):
        os.unlink(self.FOO)
        shutil.copyfile(self.BAR, self.FOO)


class TestReproDry(TestReproChangedData):
    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self.file1_stage, dry=True)

        self.assertTrue(len(stages), 2)
        self.assertFalse(filecmp.cmp(self.file1, self.BAR, shallow=False))

        ret = main(["repro", "--dry", self.file1_stage])
        self.assertEqual(ret, 0)
        self.assertFalse(filecmp.cmp(self.file1, self.BAR, shallow=False))


class TestReproUpToDate(TestRepro):
    def test(self):
        ret = main(["repro", self.file1_stage])
        self.assertEqual(ret, 0)


class TestReproDryNoExec(TestDvc):
    def test(self):
        deps = []
        for d in range(3):
            idir = "idir{}".format(d)
            odir = "odir{}".format(d)

            deps.append("-d")
            deps.append(odir)

            os.mkdir(idir)

            f = os.path.join(idir, "file")
            with open(f, "w+") as fobj:
                fobj.write(str(d))

            ret = main(
                [
                    "run",
                    "--no-exec",
                    "-d",
                    idir,
                    "-o",
                    odir,
                    "python -c 'import shutil; "
                    'shutil.copytree("{}", "{}")\''.format(idir, odir),
                ]
            )
            self.assertEqual(ret, 0)

        ret = main(["run", "--no-exec", "-f", "Dvcfile"] + deps)
        self.assertEqual(ret, 0)

        ret = main(["repro", "--dry"])
        self.assertEqual(ret, 0)


class TestReproChangedDeepData(TestReproChangedData):
    def setUp(self):
        super(TestReproChangedDeepData, self).setUp()

        self.file2 = "file2"
        self.file2_stage = self.file2 + ".dvc"
        self.dvc.run(
            fname=self.file2_stage,
            outs=[self.file2],
            deps=[self.file1, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.file1, self.file2),
        )

    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self.file2_stage)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))
        self.assertTrue(filecmp.cmp(self.file2, self.BAR, shallow=False))
        self.assertEqual(len(stages), 3)


class TestReproIgnoreBuildCache(TestDvc):
    def test(self):
        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        foo_stage = stages[0]
        self.assertTrue(foo_stage is not None)

        code1 = "code1.py"
        shutil.copyfile(self.CODE, code1)
        file1 = "file1"
        file1_stage = self.dvc.run(
            outs=[file1],
            deps=[self.FOO, code1],
            cmd="python {} {} {}".format(code1, self.FOO, file1),
        )
        self.assertTrue(file1_stage is not None)

        code2 = "code2.py"
        shutil.copyfile(self.CODE, code2)
        file2 = "file2"
        file2_stage = self.dvc.run(
            outs=[file2],
            deps=[file1, code2],
            cmd="python {} {} {}".format(code2, file1, file2),
        )
        self.assertTrue(file2_stage is not None)

        code3 = "code3.py"
        shutil.copyfile(self.CODE, code3)
        file3 = "file3"
        file3_stage = self.dvc.run(
            outs=[file3],
            deps=[file2, code3],
            cmd="python {} {} {}".format(code3, file2, file3),
        )
        self.assertTrue(file3_stage is not None)

        with open(code2, "a") as fobj:
            fobj.write("\n\n")

        stages = self.dvc.reproduce(file3_stage.path, ignore_build_cache=True)
        self.assertEqual(len(stages), 2)
        self.assertEqual(stages[0].path, file2_stage.path)
        self.assertEqual(stages[1].path, file3_stage.path)


class TestReproPipeline(TestReproChangedDeepData):
    def test(self):
        stages = self.dvc.reproduce(
            self.file1_stage, force=True, pipeline=True
        )
        self.assertEqual(len(stages), 3)

    def test_cli(self):
        ret = main(["repro", "--pipeline", "-f", self.file1_stage])
        self.assertEqual(ret, 0)


class TestReproPipelines(TestDvc):
    def setUp(self):
        super(TestReproPipelines, self).setUp()

        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.foo_stage = stages[0]
        self.assertTrue(self.foo_stage is not None)

        stages = self.dvc.add(self.BAR)
        self.assertEqual(len(stages), 1)
        self.bar_stage = stages[0]
        self.assertTrue(self.bar_stage is not None)

        self.file1 = "file1"
        self.file1_stage = self.file1 + ".dvc"
        self.dvc.run(
            fname=self.file1_stage,
            outs=[self.file1],
            deps=[self.FOO, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.FOO, self.file1),
        )

        self.file2 = "file2"
        self.file2_stage = self.file2 + ".dvc"
        self.dvc.run(
            fname=self.file2_stage,
            outs=[self.file2],
            deps=[self.BAR, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.BAR, self.file2),
        )

    def test(self):
        stages = self.dvc.reproduce(all_pipelines=True, force=True)
        self.assertEqual(len(stages), 4)
        names = [stage.relpath for stage in stages]
        self.assertTrue(self.foo_stage.relpath in names)
        self.assertTrue(self.bar_stage.relpath in names)
        self.assertTrue(self.file1_stage in names)
        self.assertTrue(self.file2_stage in names)

    def test_cli(self):
        ret = main(["repro", "-f", "-P"])
        self.assertEqual(ret, 0)


class TestReproLocked(TestReproChangedData):
    def test(self):
        file2 = "file2"
        file2_stage = file2 + ".dvc"
        self.dvc.run(
            fname=file2_stage,
            outs=[file2],
            deps=[self.file1, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.file1, file2),
        )

        self.swap_foo_with_bar()

        ret = main(["lock", file2_stage])
        self.assertEqual(ret, 0)
        stages = self.dvc.reproduce(file2_stage)
        self.assertEqual(len(stages), 0)

        ret = main(["unlock", file2_stage])
        self.assertEqual(ret, 0)
        stages = self.dvc.reproduce(file2_stage)
        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))
        self.assertTrue(filecmp.cmp(file2, self.BAR, shallow=False))
        self.assertEqual(len(stages), 3)

    def test_non_existing(self):
        with self.assertRaises(StageFileDoesNotExistError):
            self.dvc.lock_stage("non-existing-stage")

        ret = main(["lock", "non-existing-stage"])
        self.assertNotEqual(ret, 0)


class TestReproLockedCallback(TestDvc):
    def test(self):
        file1 = "file1"
        file1_stage = file1 + ".dvc"
        # NOTE: purposefully not specifying dependencies
        # to create a callbacs stage.
        stage = self.dvc.run(
            fname=file1_stage,
            outs=[file1],
            cmd="python {} {} {}".format(self.CODE, self.FOO, file1),
        )
        self.assertTrue(stage is not None)
        self.assertEqual(stage.relpath, file1_stage)

        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 1)

        self.dvc.lock_stage(file1_stage)
        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 0)

        self.dvc.lock_stage(file1_stage, unlock=True)
        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 1)


class TestReproLockedUnchanged(TestRepro):
    def test(self):
        """
        Check that locking/unlocking doesn't affect stage state
        """
        self.dvc.lock_stage(self.file1_stage)
        stages = self.dvc.reproduce(self.file1_stage)
        self.assertEqual(len(stages), 0)

        self.dvc.lock_stage(self.file1_stage, unlock=True)
        stages = self.dvc.reproduce(self.file1_stage)
        self.assertEqual(len(stages), 0)


class TestReproMetricsAddUnchanged(TestDvc):
    def test(self):
        """
        Check that adding/removing metrics doesn't affect stage state
        """
        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        file1 = "file1"
        file1_stage = file1 + ".dvc"
        self.dvc.run(
            fname=file1_stage,
            outs_no_cache=[file1],
            deps=[self.FOO, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.FOO, file1),
        )

        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 0)

        self.dvc.metrics.add(file1)
        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 0)

        self.dvc.metrics.remove(file1)
        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 0)


class TestReproPhony(TestReproChangedData):
    def test(self):
        stage = self.dvc.run(deps=[self.file1])

        self.swap_foo_with_bar()

        self.dvc.reproduce(stage.path)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))


class TestNonExistingOutput(TestRepro):
    def test(self):
        os.unlink(self.FOO)

        with self.assertRaises(ReproductionError):
            self.dvc.reproduce(self.file1_stage)


class TestReproDataSource(TestReproChangedData):
    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self.foo_stage.path)

        self.assertTrue(filecmp.cmp(self.FOO, self.BAR, shallow=False))
        self.assertEqual(
            stages[0].outs[0].checksum, file_checksum(self.BAR)[0]
        )


class TestReproChangedDir(TestDvc):
    def test(self):
        file_name = "file"
        shutil.copyfile(self.FOO, file_name)

        stage_name = "dir.dvc"
        dir_name = "dir"
        dir_code = "dir.py"
        code = (
            'import os; import shutil; os.mkdir("{}"); '
            'shutil.copyfile("{}", os.path.join("{}", "{}"))'
        )

        with open(dir_code, "w+") as fd:
            fd.write(code.format(dir_name, file_name, dir_name, file_name))

        self.dvc.run(
            fname=stage_name,
            outs=[dir_name],
            deps=[file_name, dir_code],
            cmd="python {}".format(dir_code),
        )

        stages = self.dvc.reproduce(stage_name)
        self.assertEqual(len(stages), 0)

        os.unlink(file_name)
        shutil.copyfile(self.BAR, file_name)

        stages = self.dvc.reproduce(stage_name)
        self.assertEqual(len(stages), 1)


class TestReproChangedDirData(TestDvc):
    def test(self):
        dir_name = "dir"
        dir_code = "dir_code.py"

        with open(dir_code, "w+") as fd:
            fd.write(
                "import os; import sys; import shutil; "
                "shutil.copytree(sys.argv[1], sys.argv[2])"
            )

        stage = self.dvc.run(
            outs=[dir_name],
            deps=[self.DATA_DIR, dir_code],
            cmd="python {} {} {}".format(dir_code, self.DATA_DIR, dir_name),
        )
        self.assertTrue(stage is not None)

        stages = self.dvc.reproduce(stage.path)
        self.assertEqual(len(stages), 0)

        with open(self.DATA_SUB, "a") as fd:
            fd.write("add")

        stages = self.dvc.reproduce(stage.path)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        # Check that dvc indeed registers changed output dir
        shutil.move(self.BAR, dir_name)
        stages = self.dvc.reproduce(stage.path)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        # Check that dvc registers mtime change for the directory.
        System.hardlink(self.DATA_SUB, self.DATA_SUB + ".lnk")
        stages = self.dvc.reproduce(stage.path)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)


class TestReproMissingMd5InStageFile(TestRepro):
    def test(self):
        d = load_stage_file(self.file1_stage)
        del d[Stage.PARAM_OUTS][0][RemoteLOCAL.PARAM_CHECKSUM]
        del d[Stage.PARAM_DEPS][0][RemoteLOCAL.PARAM_CHECKSUM]
        dump_stage_file(self.file1_stage, d)

        stages = self.dvc.reproduce(self.file1_stage)
        self.assertEqual(len(stages), 1)


class TestCmdRepro(TestReproChangedData):
    def test(self):
        self.swap_foo_with_bar()

        ret = main(["status"])
        self.assertEqual(ret, 0)

        ret = main(["repro", self.file1_stage])
        self.assertEqual(ret, 0)

        ret = main(["repro", "non-existing-file"])
        self.assertNotEqual(ret, 0)


class TestCmdReproChdirCwdBackwardCompatible(TestDvc):
    def test(self):
        dname = "dir"
        os.mkdir(dname)
        foo = os.path.join(dname, self.FOO)
        bar = os.path.join(dname, self.BAR)
        code = os.path.join(dname, self.CODE)
        shutil.copyfile(self.FOO, foo)
        shutil.copyfile(self.CODE, code)

        ret = main(
            [
                "run",
                "-f",
                "Dvcfile",
                "-c",
                dname,
                "-d",
                self.FOO,
                "-o",
                self.BAR,
                "python {} {} {}".format(self.CODE, self.FOO, self.BAR),
            ]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(foo))
        self.assertTrue(os.path.isfile(bar))
        self.assertTrue(filecmp.cmp(foo, bar, shallow=False))

        os.unlink(bar)

        ret = main(["repro", "-c", dname])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(foo))
        self.assertTrue(os.path.isfile(bar))
        self.assertTrue(filecmp.cmp(foo, bar, shallow=False))


class TestCmdReproChdir(TestDvc):
    def test(self):
        dname = "dir"
        os.mkdir(dname)
        foo = os.path.join(dname, self.FOO)
        bar = os.path.join(dname, self.BAR)
        code = os.path.join(dname, self.CODE)
        shutil.copyfile(self.FOO, foo)
        shutil.copyfile(self.CODE, code)

        ret = main(
            [
                "run",
                "-f",
                "{}/Dvcfile".format(dname),
                "-w",
                "{}".format(dname),
                "-d",
                self.FOO,
                "-o",
                self.BAR,
                "python {} {} {}".format(self.CODE, self.FOO, self.BAR),
            ]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(foo))
        self.assertTrue(os.path.isfile(bar))
        self.assertTrue(filecmp.cmp(foo, bar, shallow=False))

        os.unlink(bar)

        ret = main(["repro", "-c", dname])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(foo))
        self.assertTrue(os.path.isfile(bar))
        self.assertTrue(filecmp.cmp(foo, bar, shallow=False))


class TestReproShell(TestDvc):
    def test(self):
        if os.name == "nt":
            return

        fname = "shell.txt"
        stage = fname + ".dvc"

        self.dvc.run(
            fname=stage, outs=[fname], cmd="echo $SHELL > {}".format(fname)
        )

        with open(fname, "r") as fd:
            self.assertEqual(os.getenv("SHELL"), fd.read().strip())

        os.unlink(fname)

        self.dvc.reproduce(stage)

        with open(fname, "r") as fd:
            self.assertEqual(os.getenv("SHELL"), fd.read().strip())


class TestReproNoSCM(TestRepro):
    def test(self):
        shutil.rmtree(self.dvc.scm.dir)
        ret = main(["repro", self.file1_stage])
        self.assertEqual(ret, 0)


class TestReproAllPipelines(TestDvc):
    def test(self):
        self.dvc.run(
            fname="start.dvc", outs=["start.txt"], cmd="echo start > start.txt"
        )

        self.dvc.run(
            fname="middle.dvc",
            deps=["start.txt"],
            outs=["middle.txt"],
            cmd="echo middle > middle.txt",
        )

        self.dvc.run(
            fname="final.dvc",
            deps=["middle.txt"],
            outs=["final.txt"],
            cmd="echo final > final.txt",
        )

        self.dvc.run(
            fname="disconnected.dvc",
            outs=["disconnected.txt"],
            cmd="echo other > disconnected.txt",
        )

        with patch.object(Stage, "reproduce") as mock_reproduce:
            ret = main(["repro", "--all-pipelines"])
            self.assertEqual(ret, 0)
            self.assertEqual(mock_reproduce.call_count, 4)


class TestReproNoCommit(TestRepro):
    def test(self):
        shutil.rmtree(self.dvc.cache.local.cache_dir)
        ret = main(["repro", self.file1_stage, "--no-commit"])
        self.assertEqual(ret, 0)
        self.assertEqual(len(os.listdir(self.dvc.cache.local.cache_dir)), 0)


class TestReproAlreadyCached(TestRepro):
    def test(self):
        run_out = self.dvc.run(
            fname="datetime.dvc",
            deps=[],
            outs=["datetime.txt"],
            cmd='python -c "import time; print(time.time())" > datetime.txt',
        ).outs[0]

        repro_out = self.dvc.reproduce(target="datetime.dvc")[0].outs[0]

        self.assertNotEqual(run_out.checksum, repro_out.checksum)

    def test_force_with_dependencies(self):
        run_out = self.dvc.run(
            fname="datetime.dvc",
            deps=[self.FOO],
            outs=["datetime.txt"],
            cmd='python -c "import time; print(time.time())" > datetime.txt',
        ).outs[0]

        ret = main(["repro", "--force", "datetime.dvc"])
        self.assertEqual(ret, 0)

        repro_out = Stage.load(self.dvc, "datetime.dvc").outs[0]

        self.assertNotEqual(run_out.checksum, repro_out.checksum)

    def test_force_import(self):
        ret = main(["import", self.FOO, self.BAR])
        self.assertEqual(ret, 0)

        patch_download = patch.object(
            RemoteLOCAL,
            "download",
            side_effect=RemoteLOCAL.download,
            autospec=True,
        )

        patch_checkout = patch.object(
            OutputBase,
            "checkout",
            side_effect=OutputBase.checkout,
            autospec=True,
        )

        with patch_download as mock_download:
            with patch_checkout as mock_checkout:
                ret = main(["repro", "--force", "bar.dvc"])
                self.assertEqual(ret, 0)
                self.assertEqual(mock_download.call_count, 1)
                self.assertEqual(mock_checkout.call_count, 0)


class TestShouldDisplayMetricsOnReproWithMetricsOption(TestDvc):
    def test(self):
        metrics_file = "metrics_file"
        metrics_value = 0.123489015
        ret = main(
            [
                "run",
                "-m",
                metrics_file,
                "echo {} >> {}".format(metrics_value, metrics_file),
            ]
        )
        self.assertEqual(0, ret)

        self._caplog.clear()
        ret = main(
            [
                "repro",
                "--force",
                "--metrics",
                metrics_file + Stage.STAGE_FILE_SUFFIX,
            ]
        )
        self.assertEqual(0, ret)

        expected_metrics_display = "{}: {}".format(metrics_file, metrics_value)
        self.assertIn(expected_metrics_display, self._caplog.text)


@pytest.fixture
def foo_copy(repo_dir, dvc):
    stages = dvc.add(repo_dir.FOO)
    assert len(stages) == 1
    foo_stage = stages[0]
    assert foo_stage is not None

    fname = "foo_copy"
    stage_fname = fname + ".dvc"
    dvc.run(
        fname=stage_fname,
        outs=[fname],
        deps=[repo_dir.FOO, repo_dir.CODE],
        cmd="python {} {} {}".format(repo_dir.CODE, repo_dir.FOO, fname),
    )
    return {"fname": fname, "stage_fname": stage_fname}


def test_dvc_formatting_retained(dvc, foo_copy):
    root = Path(dvc.root_dir)
    stage_file = root / foo_copy["stage_fname"]

    # Add comments and custom formatting to stage file
    lines = list(map(_format_dvc_line, stage_file.read_text().splitlines()))
    lines.insert(0, "# Starting comment")
    stage_text = "".join(l + "\n" for l in lines)
    stage_file.write_text(stage_text)

    # Rewrite data source and repro
    (root / "foo").write_text("new_foo")
    dvc.reproduce(foo_copy["stage_fname"])

    # All differences should be only about md5
    assert _hide_md5(stage_text) == _hide_md5(stage_file.read_text())


def _format_dvc_line(line):
    # Add line comment for all cache and md5 keys
    if "cache:" in line or "md5:" in line:
        return line + " # line comment"
    # Format command as one word per line
    elif line.startswith("cmd: "):
        pre, command = line.split(None, 1)
        return pre + " >\n" + "\n".join("  " + s for s in command.split())
    else:
        return line


def _hide_md5(text):
    return re.sub(r"\b[a-f0-9]{32}\b", "<md5>", text)
