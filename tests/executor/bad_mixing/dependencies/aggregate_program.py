from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput


class AggregateProgram(AggregateTask):
    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {

        }

    def deaggregate(self) -> dict:
        return self.input

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    def run(self):
        pass
