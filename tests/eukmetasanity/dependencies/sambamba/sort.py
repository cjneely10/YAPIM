from typing import List, Union, Type

from yapim import Task, DependencyInput, prefix


class SambambaSort(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "sorted_bams": [self.wdir.joinpath(prefix(bam_file) + ".sorted.bam")
                            for bam_file in self.input["SambambaView"]["bams"]]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [
            DependencyInput("SambambaView")
        ]

    def run(self):
        """
        Run sambamba.sort
        """
        for bam_file, sorted_bam_file in zip(self.input["SambambaView"]["bams"], self.output["sorted_bams"]):
            self.parallel(
                self.program[
                    "sort",
                    "-t", self.threads,
                    "-o", sorted_bam_file,
                    "-m", self.memory + "GB",
                    bam_file,
                    (*self.added_flags)
                ]
            )
