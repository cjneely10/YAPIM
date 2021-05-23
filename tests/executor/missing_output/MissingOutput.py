from typing import List

from yapm import Task, DependencyInput


class MissingOutput(Task):
    @staticmethod
    def requires() -> List[str]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        open(self.wdir.joinpath("meow.txt"), "a").close()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath("out.txt")
        }
