from typing import List

from yapim import Task, DependencyInput


class Sed(Task):
    @staticmethod
    def requires() -> List[str]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath("result.sed")
        }

    def run(self):
        self.single(
            self.program[rf"s/{str(self.record_id)}/{str(self.record_id) + 'a'}/g", self.input["file"]] > str(
                self.output["result"])
        )
