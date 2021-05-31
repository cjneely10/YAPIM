import os
from typing import List, Union, Type

from yapim import Task, DependencyInput


# TODO: Make part of dependencies
class Merge(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {"final": []}
        if os.path.exists(str(self.input["AbinitioGeneMark"]["ab-gff3"])):
            self.output["prot-genemark"] = self.input["AbinitioGeneMark"]["ab-gff3"]
            self.output["gff3-genemark"] = self.input["AbinitioGeneMark"]["prot"]
            self.output["final"].append("prot-genemark")
        if os.path.exists(str(self.input["AbinitioAugustus"]["ab-gff3"])):
            self.output["gff3-augustus"] = self.input["AbinitioAugustus"]["ab-gff3"]
            self.output["gff3-augustus"] = self.input["AbinitioAugustus"]["prot"]
            self.output["final"].append("prot-augustus")

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["AbinitioAugustus", "AbinitioGeneMark"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        pass
