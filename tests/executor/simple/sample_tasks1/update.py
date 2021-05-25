from typing import List

from yapim import Task, DependencyInput


class Update(Task):
    @staticmethod
    def requires() -> List[str]:
        return ["Write"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("Sed", {"Write": {"result": "file"}})]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.input["Sed"]["result"],
            "final": ["result"]
        }

    def run(self):
        pass
