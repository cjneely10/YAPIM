from typing import List

from yapim import AggregateTask, DependencyInput


class MMSeqsConcatDB(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "out": str(self.wdir.joinpath("merged-db")),
            "final": ["out"]
        }

    def aggregate(self):
        return [str(self.input[record_id]["MMSeqsCreateDB"]["db"]) for record_id in self.input.keys()]

    def deaggregate(self) -> dict:
        return {
            key: self.output["out"]
            for key in self.input.keys()
            if key != "dbs"
        }

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    @staticmethod
    def requires() -> List[str]:
        return ["MMSeqsCreateDB"]

    def run(self):
        dbs = self.aggregate()
        self.single(
            self.program[
                "mergedbs",
                dbs[0], self.output["out"], (*dbs[1:])
            ]
        )
