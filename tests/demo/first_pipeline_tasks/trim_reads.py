from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput


class TrimReads(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def aggregate(self) -> dict:
        pass

    def deaggregate(self) -> dict:
        pass

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        pass
