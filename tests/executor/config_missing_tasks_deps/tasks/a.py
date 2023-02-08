from typing import List, Union, Type

from yapim import Task, DependencyInput, touch


class A(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "out": self.wdir.joinpath(f"{self.record_id}.out")
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        touch(self.output["out"])
