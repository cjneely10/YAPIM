from typing import List

from yapim import Task, DependencyInput


class MMSeqsCreateDB(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "db": self.wdir.joinpath(self.record_id + "-db")
        }

    @staticmethod
    def requires() -> List[str]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        self.parallel(
            self.program[
                "createdb", str(self.input["fasta"]), str(self.output["db"])
            ]
        )
