from typing import List, Union, Type

from yapim import DependencyInput, touch, AggregateTask


class Second(AggregateTask):
    def aggregate(self) -> dict:
        pass

    def deaggregate(self) -> dict:
        pass

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath(self.record_id + "-" + self.name + ".txt")
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["First"]

    def run(self):
        touch(str(self.output["result"]))
        fp = open(self.output["result"], "w")
        fp.close()

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("Program")]
