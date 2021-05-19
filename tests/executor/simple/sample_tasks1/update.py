from src import Task, DependencyInput


class Update(Task):
    task_name = "update"
    requires = ["write"]
    depends = [DependencyInput("sed", {"write": {"result": "file"}})]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.input["sed"]["result"],
            "final": ["result"]
        }

    def run(self):
        pass
