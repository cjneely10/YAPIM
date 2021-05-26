import os
from typing import List, Union, Type

from yapim import Task, DependencyInput


class Kofamscan(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {

        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        """
        Run kofamscan.exec_annotation
        """
        self.parallel(
            self.program[
                "--cpu", self.threads,
                "--format", "detail",
                "-o", self.output["kegg"],
                "--tmp-dir", os.path.join(self.wdir, "tmp"),
                self.input["prot"],
                "-p", self.config["profiles"],
                "-k", self.config["kolist"]
            ]
        )