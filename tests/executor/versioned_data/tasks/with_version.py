from typing import List, Union, Type

from yapim import Task, DependencyInput, VersionInfo


class VersionedTask(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def versions(self) -> List[VersionInfo]:
        return [VersionInfo("8.30", "--version")]

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        file = str(self.wdir.joinpath("quick.txt"))
        with open(file, "w") as file_ptr:
            file_ptr.write("Hello from versioned task!\n")
        self.single(self.program[file])
