from typing import List, Union, Type

from yapim import Task, DependencyInput


class MultiplyByTwo(Task):
    def condition(self) -> bool:
        # Only run on even input
        return int(self.record_id) % 2 == 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "byTwo": self.input["InitialTask"]["id"] * 2
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["InitialTask"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        pass
