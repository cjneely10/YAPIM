from src.tasks.task import Task, set_complete, program_catch


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

    @program_catch
    def run(self):
        (self.program[rf"s/{self.record_id}/{self.record_id + 'a'}/g", self.input["file"]] > str(self.output["result"]))()
