from dvc.package.github import PackageGitHub
from dvc.package.gitlab import PackageGitLab
from dvc.package.gitlocal import PackageGitLocal
from dvc.package.local import PackageLocal


PACKAGES = [
    PackageGitHub,
    PackageGitLab,
    PackageGitLocal
    # NOTE: PackageLocal is the default
]


def _get(config):
    for package in PACKAGES:
        if package.supported(config):
            return package
    return PackageLocal


def Package(repo, config):
    return _get(config)(repo, config)
