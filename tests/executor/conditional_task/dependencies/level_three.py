from typing import List, Union, Type

from yapim import Task, DependencyInput


class LevelThree(Task):
    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        assert self.input["id"] % 2 == 0, self.input["id"]
