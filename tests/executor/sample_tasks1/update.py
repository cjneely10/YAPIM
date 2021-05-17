from src.tasks.task import Task, set_complete
from src.tasks.utils.dependency_input import DependencyInput


class Update(Task):
    task_name = "update"
    requires = ["write"]
    depends = [DependencyInput("sed", {"write": {"result": "file"}})]

    @set_complete
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.input["sed"]["result"],
            "final": ["result"]
        }

    def run(self):
        pass
