from src.tasks.task import Task


class Cat(Task):
    task_name = "cat"
    requires = ["echo"]
    depends = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.input["echo"]["result"],
            "final": ["result"]
        }

    def run(self):
        print(self.input["echo"]["result"])
