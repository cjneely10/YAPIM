import os
from typing import List, Union, Type

from yapim import Task, DependencyInput, prefix


class SambambaSort(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "sorted.bams": [os.path.join(self.wdir, prefix(db) + ".sorted.bam")
                            for db in self.input["sambamba.view"]["bams"]]
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
        for bam_file in self.output["sorted.bams"]:
            out_prefix = os.path.splitext(bam_file)[0]
            self.parallel(
                self.program[
                    "sort",
                    "-t", self.threads,
                    "-o", out_prefix + ".sorted.bam",
                    bam_file,
                    (*self.added_flags)
                ]
            )
