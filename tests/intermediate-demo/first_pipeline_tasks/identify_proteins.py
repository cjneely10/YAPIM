from typing import List, Union, Type

from yapim import Task, DependencyInput


class IdentifyProteins(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "proteins": str(self.wdir.joinpath(self.record_id + ".faa")),
            "fasta": self.input["fasta"],
            "final": ["proteins", "fasta"]
        }

    # Error: prodigal has first line as empty, stdout not catching subsequent version info
    # def versions(self) -> List[VersionInfo]:
    #     return [VersionInfo(calling_parameter="-v", version="2.6.3")]

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["QualityCheck"]

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
