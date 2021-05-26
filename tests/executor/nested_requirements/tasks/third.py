from typing import List, Union, Type

from yapim import Task, DependencyInput


class Third(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.input["Second"]["result"],
            "final": ["result"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["Second"]

    def run(self):
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("Program")]
