import shutil
from typing import List, Union, Type

from yapim import Task, DependencyInput, VersionInfo


class Assemble(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "fasta": self.wdir.joinpath("tmp").joinpath("final.contigs.fa")
        }

    def versions(self) -> List[VersionInfo]:
        return [VersionInfo("1.2.9", "-v")]

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["TrimReads"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        out_dir = self.wdir.joinpath("tmp")
        if out_dir.exists():
            shutil.rmtree(out_dir)
        self.parallel(
            self.program[
                "-1", self.input["TrimReads"]["PE1"],
                "-2", self.input["TrimReads"]["PE2"],
                (*self.added_flags),
                "-t", self.threads,
                "-m", round(int(self.memory) * 1E9),
                "-o", out_dir
            ]
        )
