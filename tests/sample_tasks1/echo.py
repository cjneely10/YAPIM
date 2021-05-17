from src.tasks.task import Task


class Echo(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath("result.txt"),
            "final": ["result"]
        }

    task_name = "echo"

    requires = []

    depends = []

    def run(self):
        (self.local["echo"]["Hello world!"] > str(self.output["result"]))()
