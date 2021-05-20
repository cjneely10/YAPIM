from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Tuple, Union, Type

from HPCBioPipe.tasks.utils.dependency_input import DependencyInput


class BaseTask(ABC):
    class TaskCompletionError(Exception):
        def __init__(self, task_name: str, file: Path):
            super().__init__(f"Task {task_name} output file missing {file}")

    @property
    def full_name(self) -> Tuple[str, str]:
        """
        :return: Tuple of (scope, task name)
        """
        return self.task_scope(), type(self).__name__

    @property
    def name(self) -> str:
        return type(self).__name__

    @staticmethod
    @abstractmethod
    def task_scope() -> str:
        """

        :return:
        :rtype:
        """
    @staticmethod
    @abstractmethod
    def requires() -> List[Union[str, Type]]:
        """ List of tasks whose outputs are used in this task.

        :return: List of Task child classes
        """

    @staticmethod
    @abstractmethod
    def depends() -> List[DependencyInput]:
        """ List of programs to run to generate intermediary output for this task

        :return: List of Task child classes
        """

    @abstractmethod
    def run(self):
        """ Implementation of method to run to complete a given task

        :return:
        """
