from typing import List, Union, Type

from yapim import Task, DependencyInput


class Program(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath(self.record_id + ".txt")
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["First"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        fp = open(self.output["result"], "a")
        fp.write(str(self.input["First"]["result"]))
        fp.write("\n")
        fp.close()
