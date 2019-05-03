from dvc.config import Config


from dvc.pkg.base import PackageBase


class PackageGIT(PackageBase):
    def install(self):
        import git

        git.repo.clone_from(self.url, self.dir)
