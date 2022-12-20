from typing import List, Union, Type

from yapim import Task, DependencyInput


class Combine(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(self.input)
        self.output = {
            "out_file": self.wdir.joinpath(self.input["value"]),
            "final": ["out_file"]
        }

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    def run(self):
        with open(self.output["out_file"], "w") as f_ptr:
            f_ptr.write(self.input["new_output"])
            f_ptr.write("\n")
            f_ptr.write(self.input["output"])
            f_ptr.write("\n")
            f_ptr.write(self.input["update-result"])
