from typing import List, Union, Type

from yapim import Task, DependencyInput


class RunBraker(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "prot": self.input["Braker"]["prot"],
            "gff3": self.input["Braker"]["gff3"],
            "cds": self.input["Braker"]["cds"],
            "final": ["prot", "gff3", "cds"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["MergeBams", "GatherProteins"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("Braker", {"MergeBams": ["bams"], "GatherProteins": ["prots"]})]

    def run(self):
        pass
