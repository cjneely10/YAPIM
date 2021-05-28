from typing import List, Union, Type

from yapim import Task, DependencyInput


class MMSeqs(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "results": self.input["MMSeqsConvertAlis"]["results_files"][0],
            "final": ["results"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("MMSeqsConvertAlis", {"root": {"prot": "fasta"}})]

    def run(self):
        pass
