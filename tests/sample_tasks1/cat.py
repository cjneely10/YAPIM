from typing import List

from src.tasks.task import Task
from src.tasks.utils.dependency_input import DependencyInput


class Cat(Task):
    task_name = "cat"

    requires = ["echo"]

    depends = []

    def run(self):
        self.local["cat"][self.input["echo"]["result"]]()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.input["echo"]["result"],
            "final": ["result"]
        }
