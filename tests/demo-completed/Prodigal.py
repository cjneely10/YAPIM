from typing import List, Union, Type

from yapim import Task, DependencyInput


class Prodigal(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "prot": self.wdir.joinpath(f"{self.record_id}.faa"),
            "cds": self.wdir.joinpath(f"{self.record_id}.cds.fna"),
            "gff": self.wdir.joinpath(f"{self.record_id}.gff"),
            "final": ["prot", "cds", "gff"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["CheckM"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        self.single(
            self.program[
                "-i", self.input["fasta"],
                "-o", self.output["gff"],
                "-a", self.output["prot"],
                "-d", self.output["cds"],
                "-f", "gff",
                "-p", "meta",
                (*self.added_flags)
            ]
        )
