from typing import List, Union, Type

from yapim import Task, DependencyInput, Result
from yapim.tasks.task import TaskExecutionError


class Dependency(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "output": Result(self.record_id + "-dependency")
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        print(self.input)
        if self.input["input"] != "other":
            raise TaskExecutionError()
