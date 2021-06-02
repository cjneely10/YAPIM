from typing import List

from yapim import Task, DependencyInput


class Update(Task):
    @staticmethod
    def requires() -> List[str]:
        return ["Write"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("Sed", {"Write": {"write-result": "file"}})]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "update-result": self.input["Sed"]["result"],
            "final": ["update-result"]
        }

    def run(self):
        pass
