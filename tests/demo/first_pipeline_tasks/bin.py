import os
from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput, prefix, clean


class Bin(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "bins": self.wdir.joinpath("tmp").joinpath("bins")
        }

    def deaggregate(self) -> dict:
        self.remap()
        return {
            prefix(file): {
                "fasta": self.output["bins"].joinpath(file)
            }
            for file in os.listdir(self.output["bins"])
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["MapBackReads", "CatalogueReads"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    @clean("tmp")
    def run(self):
        bams = [
            self.input[key]["MapBackReads"]["bam"]
            for key in self.input_ids()
            if self.has_run("MapBackReads", key)
        ]
        self.parallel(
            self.program[
                "--outdir", self.wdir.joinpath("tmp"),
                "--fasta", self.input["CatalogueReads"]["fasta_catalogue"],
                "--bamfiles", (*bams),
                "-o", "_",
                (*self.added_flags)
            ]
        )
