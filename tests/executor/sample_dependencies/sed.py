from src.tasks.task import Task, set_complete


class Sed(Task):
    task_name = "sed"
    requires = []
    depends = []

    @set_complete
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath("result.sed")
        }

    def run(self):
        self.single(
            self.program[rf"s/{self.record_id}/{self.record_id + 'a'}/g", self.dependency_input["file"]] > str(
                self.output["result"])
        )
