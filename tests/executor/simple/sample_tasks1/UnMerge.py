from typing import List, Union, Type

from HPCBioPipe import Task, DependencyInput


class UnMerge(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["Merge"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        print(self.record_id)
