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
        (self.program[rf"s/{self.record_id}/{self.record_id + 'a'}/g", self.input["file"]] > str(self.output["result"]))()
