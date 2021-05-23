import os
from typing import List, Union, Type

from HPCBioPipe import AggregateTask


class CheckM(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "CheckMResult": self.wdir.joinpath("checkm.log"),
            "final": ["CheckMResult"]
        }

    def run(self):
        self.parallel(
            self.program[
                "lineage_wf",
                "-t", self.threads,
                "-x", ".fna",
                self.input["input"],
                str(self.wdir)
            ]
        )

    def aggregate(self) -> dict:
        key = list(self.input.keys())[0]
        return {
            "input": os.path.dirname(self.input[key]["fasta"])
        }

    def deaggregate(self) -> dict:
        return {
            record_id: {}
            for record_id in self.input.keys()
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []
