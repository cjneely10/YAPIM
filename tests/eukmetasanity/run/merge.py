from typing import List, Union, Type

from yapim import Task, DependencyInput


class Merge(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "merged-prot": str(self.wdir.joinpath(self.record_id + ".faa")),
            "merged-gff3": str(self.wdir.joinpath(self.record_id + ".gff3")),
            "final": ["merged-prot", "merged-gff3"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["AbinitioGeneMark", "AbinitioAugustus", "Evidence"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.single(
            self.program[
                "-g", self.input["fasta"],
                "-i", self.input["AbinitioGeneMark"]["genemark-gff3"],
                "-i", self.input["AbinitioAugustus"]["aug-gff3"],
                "-i", self.input["Evidence"]["evidence-gff3"],
                "-o", self.output["merged-gff3"]
            ]
        )
        self.single(
            self.local["gffread"][
                "-g", self.input["fasta"],
                self.output["merged-gff3"],
                "-y", self.output["merged-prot"]
            ]
        )