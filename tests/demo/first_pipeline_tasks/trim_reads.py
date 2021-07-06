from typing import List, Union, Type

from yapim import DependencyInput
from yapim import Task, VersionInfo


class TrimReads(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "PE1": self.wdir.joinpath("PE1.fastq"),
            "PE2": self.wdir.joinpath("PE2.fastq"),
        }

    def versions(self) -> List[VersionInfo]:
        return [VersionInfo("0.39", "-version")]

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.parallel(
            self.program[
                "PE",
                "-threads", self.threads,
                "-phred33",
                (*self.added_flags),
                self.input["fastq_1"],
                self.input["fastq_2"],
                self.output["PE1"], "/dev/null",
                self.output["PE2"], "/dev/null",
                (*self.flags_to_list("TRIM_FLAGS"))
            ]
        )
