from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput, Result


class Program(AggregateTask):
    def deaggregate(self) -> dict:
        return self.input

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": [Result(self.input[record_id]) for record_id in self.input_ids()],
            "result_file": self.wdir.joinpath("result.txt")
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["First"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        fp = open(str(self.output["result_file"]), "a")
        fp.write(str(self.input_ids()))
        fp.write("\n")
        fp.close()
