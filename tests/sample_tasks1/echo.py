from src.tasks.task import Task, set_complete


class Echo(Task):
    task_name = "echo"
    requires = []
    depends = []

    @set_complete
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath("result.txt"),
            "final": ["result"]
        }

    def run(self):
        self.single(
            self.local["echo"][self.record_id] > str(self.output["result"])
        )
