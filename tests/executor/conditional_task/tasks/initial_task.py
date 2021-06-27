from typing import List, Union, Type

from yapim import Task, DependencyInput


class InitialTask(Task):
    def condition(self) -> bool:
        # Only run on even input
        return int(self.record_id) % 2 == 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "id": int(self.record_id)
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        print(self.output["id"])
