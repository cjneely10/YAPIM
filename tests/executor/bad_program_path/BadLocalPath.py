from typing import List

from yapim import Task, DependencyInput


class BadLocalPath(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def requires() -> List[str]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        self.local["asdf"]()
