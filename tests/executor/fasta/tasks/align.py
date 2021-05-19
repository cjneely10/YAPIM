from typing import List

from src import Task, DependencyInput


class Align(Task):
    @staticmethod
    def requires() -> List[str]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        open(self.output["align"], "a").close()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "align": self.wdir.joinpath("align.txt")
        }
