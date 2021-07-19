from typing import List, Union, Type

from yapim import Task, DependencyInput


class CallAnnotate(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.input["Annotate"]["result"],
            "proteins": self.input["IdentifyProteins"]["proteins"],
            "final": ["result", "proteins"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["IdentifyProteins", "QualityCheck"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("Annotate", {"IdentifyProteins": ["proteins"]})]

    def run(self):
        pass
