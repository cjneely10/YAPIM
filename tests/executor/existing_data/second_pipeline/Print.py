from typing import List, Union, Type

from HPCBioPipe import Task, DependencyInput


class Print(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "output": self.wdir.joinpath("out.txt"),
            "final": ["output"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        print(self.input)
        open(self.output["output"], "a").close()
