import os
from typing import List, Union, Type

from yapim import Task, DependencyInput


class EMapper(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        """
        Run emapper
        """
        self.parallel(
            self.local[self.config["python"]][
                self.program,
                "-i", self.input["prot"],
                "--output", os.path.join(self.wdir, self.record_id),
                "--cpu", self.threads,
                (*self.added_flags),
            ]
        )
        os.replace(
            os.path.join(self.wdir, self.record_id + ".emapper.annotations"),
            str(self.output["emapper"])
        )
