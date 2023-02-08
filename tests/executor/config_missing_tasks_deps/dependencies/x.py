from typing import List, Union, Type

from yapim import Task, DependencyInput


class X(Task):
    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        pass
