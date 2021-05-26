from typing import List, Union, Type

from yapim import Task, DependencyInput


class SingleTask(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {

        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("AggregateProgram")]

    def run(self):
        pass
