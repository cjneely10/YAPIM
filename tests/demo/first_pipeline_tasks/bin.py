import os
import shutil
from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput, prefix


class Bin(AggregateTask):
    def deaggregate(self) -> dict:
        bins_dir = self.wdir.joinpath("tmp").joinpath("bins")
        self.remap()
        return {
            prefix(file): {
                "fasta": bins_dir.joinpath(file)
            }
            for file in os.listdir(bins_dir)
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["MapBackReads", "CatalogueReads"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        out_dir = self.wdir.joinpath("tmp")
        if out_dir.exists():
            shutil.rmtree(out_dir)
        bams = [
            self.input[key]["MapBackReads"]["bam"]
            for key in self.input_ids()
            if "MapBackReads" in self.input[key].keys()
        ]
        self.parallel(
            self.program[
                "--outdir", out_dir,
                "--fasta", self.input["CatalogueReads"]["fasta_catalogue"],
                "--bamfiles", (*bams),
                "-o", "_",
                (*self.added_flags)
            ]
        )
