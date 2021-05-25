from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput


class Quality(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.input["CheckM"]["CheckMResult"],
            "final": ["result"]
        }

    def aggregate(self) -> dict:
        return {}

    def deaggregate(self) -> dict:
        return self.input

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("CheckM")]

    def run(self):
        pass
