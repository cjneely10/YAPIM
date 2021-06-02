from typing import List, Union, Type

from yapim import Task, DependencyInput, Result


class Second(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "output": Result(self.input["Dependency"]["output"])
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["First"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("Dependency", {"root": {"other": "input"}})]

    def run(self):
        pass
