"""
Holds functionality for ABC Task implementation. Expose shared functionality to allow user-definitions at API level
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Tuple, Union, Type

from plumbum import colors

from yapim.tasks.utils.dependency_input import DependencyInput


class BaseTask(ABC):
    """Top-level ABC functionality that allows parsing into dependency graph and top-level API functionality"""
    class TaskCompletionError(Exception):
        """Exception when a Task output was unsuccessfully generated after completion of this Task's run() method"""
        def __init__(self, task_name: str, attr_name: str, file: Path):
            """Call Exception superclass with formatted message"""
            # pylint: disable=no-member
            super().__init__(colors.red & colors.bold |
                             f"Task <{task_name}> output id <{attr_name}> is missing its file <{file}>")

    @property
    def full_name(self) -> Tuple[str, str]:
        """
        :return: Tuple of (scope, task name)
        """
        return self.task_scope(), type(self).__name__

    @property
    def name(self) -> str:
        """Get name of class"""
        return type(self).__name__

    @staticmethod
    @abstractmethod
    def task_scope() -> str:
        """Task level - either ROOT or the name of a pipeline Task/AggregateTask"""

    @staticmethod
    @abstractmethod
    def requires() -> List[Union[str, Type]]:
        """ List of Task classes or names that must be run before this task can run.
        Tasks listed in the requires() method will be available to instances of Task subclasses.
        """

    @staticmethod
    @abstractmethod
    def depends() -> List[DependencyInput]:
        """ List of programs to run to generate intermediary output for this task.

        The `DependencyInput` initializer requires a name or type. Optionally, provide overwrite information:

        def requires() -> List[Union[str, Type]]:
            return ["A", "B"]


        def depends() -> List[DependencyInput]:
            return [
                DependencyInput("DependencyName", {"A": {"from": "to"}, "B": ["from"]})
            ]
        """

    @abstractmethod
    def run(self):
        """Implementation of method to run to complete a given task"""
