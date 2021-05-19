from src import Task


class Write(Task):
    task_name = "write"
    requires = []
    depends = []

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
