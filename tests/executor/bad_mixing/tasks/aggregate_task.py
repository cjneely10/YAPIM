from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput


class AggTask(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {

        }

    def aggregate(self) -> dict:
        return self.input

    def deaggregate(self) -> dict:
        return self.input

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("Program")]

    def run(self):
        pass
