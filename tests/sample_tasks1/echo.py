from src.tasks.task import Task


class Echo(Task):
    task_name = "echo"
    requires = []
    depends = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath("result.txt"),
            "final": ["result"]
        }

    def run(self):
        (self.local["echo"][self.record_id] > str(self.output["result"]))()
