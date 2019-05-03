import shutil

from .base import PackageBase
from dvc.remote.local import RemoteLOCAL


class PackageLocal(PackageBase):
    @classmethod
    def supported(cls, config):
        return True

    def install(self):
        shutil.copytree(self.url, self.dir)
