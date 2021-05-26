from typing import List, Union, Type

from yapim import Task, DependencyInput, Result


class First(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath(self.record_id + "-" + self.name + ".txt"),
            "second-result": Result(self.record_id)
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    def run(self):
        fp = open(self.output["result"], "a")
        fp.write(f"Hello from {self.record_id}\n")
        fp.close()

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []
