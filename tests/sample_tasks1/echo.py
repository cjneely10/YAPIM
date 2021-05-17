from typing import List

from src.tasks.task import Task
from src.tasks.utils.dependency_input import DependencyInput


class Echo(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath("result.txt")
        }

    @property
    def task_name(self) -> str:
        return "echo"

    @property
    def requires(self) -> List[str]:
        return []

    @property
    def depends(self) -> List[DependencyInput]:
        return []

    def run(self):
        (self.local["echo"]["Hello world!"] > self.output["result"])()
