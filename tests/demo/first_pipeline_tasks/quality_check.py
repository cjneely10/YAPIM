from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput


class QualityCheck(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "outdir": self.wdir.joinpath("out")
        }

    def deaggregate(self) -> dict:
        pass

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["Bin"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.parallel(
            self.program[
                "lineage_wf",
                "-t", self.threads,
                "-x", "fna",
                self.input["Bin"]["bins"],
                self.output["outdir"]
            ]
        )
