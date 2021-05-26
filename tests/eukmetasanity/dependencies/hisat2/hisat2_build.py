import os
from typing import List, Union, Type

from yapim import Task, DependencyInput


class Hisat2Build(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "db": (os.path.join(self.wdir, self.record_id + "_db") if not self.is_skip else [])
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        """
        Run hisat2-build
        """
        self.single(
            self.program[
                self.input["fasta"],
                self.output["db"]
            ],
            "30:00"
        )
