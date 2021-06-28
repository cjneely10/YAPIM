from typing import List, Union, Type

from yapim import Task, DependencyInput, VersionInfo


class MapBackReads(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "bam": self.wdir.joinpath(self.record_id + ".bam")
        }

    def versions(self) -> List[VersionInfo]:
        return [VersionInfo("2.20-r1061", "--version"), VersionInfo("0.6.6", "-v", "converter")]

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["CatalogueReads", "TrimReads"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        index_file = str(self.wdir.joinpath("catalogue.mmi"))
        # Index samples
        self.single(
            self.program[
                "-d", index_file,
                self.input["CatalogueReads"]["catalogue"]
            ]
        )
        # Map to input BAM files
        half_threads = int(self.threads) // 2
        self.parallel(
            self.program[
                "-t", half_threads,
                (*self.added_flags),
                "-ax", "sr",
                index_file,
                self.input["TrimReads"]["PE1"],
                self.input["TrimReads"]["PE2"],
            ] | self.local[self.config["converter"]]["view"][
                "-S", "/dev/stdin",
                "-t", half_threads,
                "-o", self.output["bam"],
                "-f", "bam"
            ]
        )
