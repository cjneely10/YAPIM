import os
from typing import Optional, List

from src.tasks.task import Task
from src.tasks.utils.dependency_input import DependencyInput


class Cat(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath("result.txt")
        }

    @property
    def task_name(self) -> str:
        return "cat"

    @property
    def requires(self) -> List[str]:
        return []

    @property
    def depends(self) -> Optional[List[DependencyInput]]:
        return []

    def run(self):
        (self.local["echo"]["Hello world!"] > self.output["result"])()
