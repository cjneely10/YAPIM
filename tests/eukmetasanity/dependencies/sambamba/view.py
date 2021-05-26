import os
from typing import List, Union, Type

from yapim import Task, DependencyInput, prefix


class SambambaView(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "bams": []
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        for bam_file in [os.path.join(self.wdir, prefix(db) + ".bam") for db in self.input["sams"]]:
            self.parallel(
                self.program[
                    "view",
                    "-S", os.path.splitext(bam_file)[0] + ".sam",
                    "-t", self.threads,
                    "-o", bam_file,
                    (*self.added_flags)
                ]
            )
