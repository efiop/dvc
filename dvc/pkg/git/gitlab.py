from dvc.config import Config
from .base import PackageGitBase


class PackageGitLab(PackageGitBase):
    def supported(config):
        url = config.get(Config.SECTION_PKG_URL)
        return url.startswith("https://gitlab.com/")
