from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput


class CheckM(AggregateTask):
    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "CheckMResult": self.wdir.joinpath("checkm.log"),
        }

    def run(self):
        open(self.output["CheckMResult"], "a").close()

    def deaggregate(self) -> dict:
        return {"CheckM": {}}

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []
