from typing import List, Union, Type

from yapim import Task, DependencyInput


class LevelOne(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {"id": self.input["id"]}

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("LevelTwo")]

    def run(self):
        assert self.input["id"] % 2 == 0, self.input["id"]
