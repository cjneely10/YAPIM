from typing import List

from src.tasks.task import Task
from src.tasks.utils.dependency_input import DependencyInput


class Cat(Task):
    @property
    def task_name(self) -> str:
        return "cat"

    @property
    def requires(self) -> List[str]:
        return ["echo"]

    @property
    def depends(self) -> List[DependencyInput]:
        return []

    def run(self):
        self.local["cat"][self.input["echo"]["result"]]()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.input["echo"]["result"],
            "final": ["result"]
        }
