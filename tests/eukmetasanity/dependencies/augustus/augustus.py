from typing import List, Union, Type

from yapim import Task, DependencyInput


class Augustus(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {

        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [
            DependencyInput("MMSeqsConvertAlis")
        ]

    def run(self):
        pass
