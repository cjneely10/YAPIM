from typing import List, Union, Type

from yapim import Task, DependencyInput


class IdentifyProteins(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "proteins": self.wdir.joinpath(self.record_id + ".faa"),
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.single(
            self.program[
                "-i", self.input["fasta"],
                "-a", self.output["proteins"],
                (*self.added_flags)
            ]
        )
