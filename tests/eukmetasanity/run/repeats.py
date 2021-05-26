from typing import List, Union, Type

from yapim import Task, DependencyInput


class Repeats(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "mask-fna": self.input["RMaskRMOut"]["mask-fna"],
            "rmtbl": self.input["RMaskRMOut"]["rmtbl"],
            "mask-gff3": self.input["RMaskRMOut"]["mask-gff3"],
            "final": ["rmtbl", "mask-fna", "mask-gff3"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["Taxonomy"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [
            DependencyInput("RMaskRMOut"),
            DependencyInput("RMaskProcessRepeats")
        ]

    def run(self):
        pass
