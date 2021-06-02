from typing import List, Union, Type

from yapim import Task, DependencyInput


class MergeBams(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "bams": self.input["SambambaSort"]["bams"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["RNASeq", "Transcriptomes"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("SambambaSort", {"RNASeq": ["sams"], "Transcriptomes": ["sams"]})]

    def run(self):
        pass
