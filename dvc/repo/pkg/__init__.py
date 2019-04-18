class Pkg(object):
    def __init__(self, repo):
        self.repo = repo

    def add(self, *args, **kwargs):
        from dvc.repo.pkg.add import add

        return add(self.repo, *args, **kwargs)

    def remove(self, *args, **kwargs):
        from dvc.repo.pkg.remove import remove

        return remove(self.repo, *args, **kwargs)

    def list(self, *args, **kwargs):
        from dvc.repo.pkg.list import lst

        return lst(self.repo, *args, **kwargs)
