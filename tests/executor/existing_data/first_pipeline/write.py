from typing import List

from yapim import Task, DependencyInput


class Write(Task):
    @staticmethod
    def requires() -> List[str]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "write-result": self.wdir.joinpath("result.txt"),
            "final": ["write-result"]
        }

    def run(self):
        self.single(
            self.local["echo"][self.record_id] > str(self.output["write-result"])
        )
