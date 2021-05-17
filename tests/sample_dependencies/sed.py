from src.tasks.task import Task


class Sed(Task):
    task_name = "sed"
    requires = []
    depends = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath("result.sed")
        }

    def run(self):
        (self.program["s/\n/\n./g", self.input["result"]] > str(self.output["result"]))()
