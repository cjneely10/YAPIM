from typing import List, Union, Type

from yapim import Task, DependencyInput


class Evidence(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "gff3": self.input["MetaEuk"]["gff3"],
            "prot": self.input["MetaEuk"]["prot"],
            "final": ["prot", "gff3"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [
            DependencyInput("MetaEuk")
        ]

    def run(self):
        pass
