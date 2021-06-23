from typing import List, Union, Type, Tuple

from yapim import Task, DependencyInput


class IdentifyProteins(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "proteins": str(self.wdir.joinpath(self.record_id + ".faa"))
        }

    @staticmethod
    def versions() -> List[Tuple[str, str]]:
        return [("-v", "V2.6.4")]

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

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
