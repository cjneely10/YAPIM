from src.tasks.task import Task, set_complete


class Write(Task):
    task_name = "write"
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
