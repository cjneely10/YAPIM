from typing import List, Union, Type

from yapim import Task, DependencyInput, Result
from yapim.tasks.task import TaskExecutionError


class Third(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "output": Result(self.input["Second"]["output"])
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["Second"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        print(self.input)
        if "other" in self.input["input"]:
            raise TaskExecutionError()
