from typing import List, Union, Type

from yapim import Task, DependencyInput, VersionInfo


class Assemble(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "fasta": self.wdir.joinpath("contigs.fasta")
        }

    def versions(self) -> List[VersionInfo]:
        return [VersionInfo("3.13.0", "-v")]

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["TrimReads"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.parallel(
            self.program[
                "--meta",
                self.input["TrimReads"]["PE1"],
                self.input["TrimReads"]["PE2"],
                "-k", "21,29,39,59,79,99",
                "-t", self.threads,
                "-m", str(self.memory) + "gb",
                "-o", self.wdir
            ]
        )
