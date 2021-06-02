from typing import List, Union, Type

try:
    from tasks.external_functions import add_one
except ModuleNotFoundError:
    from tests.executor.added_imports.tasks.external_functions import add_one
from yapim import Task, DependencyInput


class ComplexTask(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": -1
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.output["result"] = add_one(self.record_id)
        print(self.output["result"])
