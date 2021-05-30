import os
from typing import List, Union, Type

from yapim import Task, DependencyInput


# TODO: Make part of dependencies
class Merge(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {"final": []}
        if os.path.exists(str(self.input["AbinitioGeneMark"]["ab-gff3"])):
            self.output["prot-genemark"] = os.path.join(self.wdir, self.record_id + ".gmes.faa")
            self.output["final"].append("prot-genemark")
        if os.path.exists(str(self.input["AbinitioAugustus"]["ab-gff3"])):
            self.output["prot-augustus"] = os.path.join(self.wdir, self.record_id + ".augustus.faa")
            self.output["final"].append("prot-augustus")

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["AbinitioAugustus", "AbinitioGeneMark"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        if os.path.exists(str(self.input["AbinitioGeneMark"]["ab-gff3"])):
            self.single(
                self.local["gffread"][
                    "-g", self.input["fasta"],
                    str(self.input["AbinitioGeneMark"]["ab-gff3"]),
                    "-y", self.output["prot-genemark"]
                ]
            )
        if os.path.exists(str(self.input["AbinitioAugustus"]["ab-gff3"])):
            self.single(
                self.local["gffread"][
                    "-g", self.input["fasta"],
                    str(self.input["AbinitioAugustus"]["ab-gff3"]),
                    "-y", self.output["prot-augustus"]
                ]
            )
