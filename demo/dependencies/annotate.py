from typing import List, Union, Type

from yapim import Task, DependencyInput


class Annotate(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath("result.m8")
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.parallel(
            self.program[
                "easy-search",
                self.input["proteins"],
                self.data[0],
                self.output["result"],
                self.wdir.joinpath("tmp"),
                "--remove-tmp-files",
                "--threads", self.threads,
                (*self.added_flags),
            ]
        )