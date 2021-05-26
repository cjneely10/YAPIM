from typing import List, Union, Type

from yapim import Task, DependencyInput, touch, prefix


class Second(Task):
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
        fp.write(str(self.input["First"]["result"]) + "\n")
        fp.write(str(self.input["First"]["second-result"]) + " " + prefix(str(self.wdir)))
        fp.close()

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []
