import os

from dvc.config import Config
from .base import PackageGitBase


class PackageGitLocal(PackageGitBase):
    @classmethod
    def supported(cls, config):
        return os.path.isdir(config[Config.SECTION_PKG_URL], ".git")
