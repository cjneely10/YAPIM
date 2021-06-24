from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Tuple, Union, Type

from plumbum import colors

from yapim.tasks.utils.dependency_input import DependencyInput


class BaseTask(ABC):
    class TaskCompletionError(Exception):
        def __init__(self, task_name: str, attr_name: str, file: Path):
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
        """
        Get name of class
        """
        return type(self).__name__

    @staticmethod
    @abstractmethod
    def task_scope() -> str:
        """ Task level - either ROOT or the name of a pipeline Task/AggregateTask

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

    # @staticmethod
    # @abstractmethod
    # def versions() -> List[Version]:
    #     """ List of (command, allowed_version) of program to run this task
    #
    #     :return:
    #     :rtype:
    #     """

    @abstractmethod
    def run(self):
        """
        Implementation of method to run to complete a given task
        """
