from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput


class Bin(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {

        }

    def aggregate(self) -> dict:
        return {
            "bams": [
                self.input[key]["bams"]
                for key in self.input.keys()
            ]
        }

    def deaggregate(self) -> dict:
        pass

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["MapBackReads"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.parallel(
            self.program[
                "--outdir", self.wdir,
                "--fasta", self.input["catalogue"],
                "--bamfiles", (*self.input["bams"]),
                "-o", "C",
                (*self.added_flags)
            ]
        )
