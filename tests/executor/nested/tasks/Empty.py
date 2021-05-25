from typing import List

from yapim import Task, DependencyInput


class Empty(Task):
    @staticmethod
    def requires() -> List[str]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        with open(self.output["out"], "w") as fp:
            fp.write("Hello world!")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "out": self.wdir.joinpath("out.txt"),
            "final": ["out"]
        }
