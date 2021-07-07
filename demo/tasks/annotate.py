from typing import List, Union, Type

from yapim import Task, DependencyInput


class Annotate(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath("result.m8"),
            "proteins": self.input["IdentifyProteins"]["proteins"],
            "final": ["result", "proteins"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["IdentifyProteins", "QualityCheck"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.parallel(
            self.program[
                "easy-search",
                self.input["IdentifyProteins"]["proteins"],
                self.data[0],
                self.output["result"],
                self.wdir.joinpath("tmp"),
                "--remove-tmp-files",
                "--threads", self.threads,
                (*self.added_flags),
            ]
        )