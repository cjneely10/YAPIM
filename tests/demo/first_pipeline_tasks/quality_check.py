from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput


class QualityCheck(AggregateTask):
    def output(self) -> dict:
        pass

    def deaggregate(self) -> dict:
        pass

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["Bin"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        pass
