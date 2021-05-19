from typing import List

from src import AggregateTask, DependencyInput


class MMSeqsConcatDB(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "out": str(self.wdir.joinpath("merged-db")),
            "final": ["out"]
        }

    def aggregate(self) -> dict:
        return {
            "dbs": [str(self.input[record_id]["MMSeqsCreateDB"]["db"]) for record_id in self.input.keys()]
        }

    @staticmethod
    def requires() -> List[str]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("MMSeqsCreateDB")]

    def run(self):
        self.single(
            self.program[
                "mergedbs",
                self.input["dbs"][0], self.output["out"], (*self.input["dbs"][1:])
            ]
        )
