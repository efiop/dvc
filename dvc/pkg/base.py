import re

import shutil

from dvc.repo import Repo


def PackageBASE(object):
    PKG_DIR = "pkg"

    def __init__(self, repo, name, config):
        self.repo = repo
        self.name = name
        self.url = config.get(Config.SECTION_PKG_URL)
        self.dir = os.path.join(repo.dvc_dir, self.PKG_DIR, name)

        if not os.path.isdir(self.dir):
            self.install()

    @classmethod
    def match(cls, url):
        return re.match(cls.REGEX, url)

    @classmethod
    def supported(cls, config):
        url = config[Config.SECTION_PKG_URL]
        return cls.match(url) is not None

    def install(self):
        raise NotImplementedError

    def update(self):
        raise NotImplementedError

    def remove(self):
        shutil.rmtree(self.dir)

    @property
    def dvc(self):
        return Repo(self.dir)

    def fetch(self):
        self.dvc.fetch()
