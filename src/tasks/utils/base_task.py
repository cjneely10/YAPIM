from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Tuple

from src.tasks.utils.dependency_input import DependencyInput


class BaseTask(ABC):
    class TaskCompletionError(Exception):
        def __init__(self, task_name: str, file: Path):
            super().__init__(f"Task {task_name} output file missing {file}")

    @property
    def full_name(self) -> Tuple[str, str]:
        """
        :return: Tuple of (scope, task name)
        """
        return self.task_scope, self.task_name

    @property
    @abstractmethod
    def task_scope(self) -> str:
        """

        :return:
        :rtype:
        """

    @property
    @abstractmethod
    def task_name(self) -> str:
        """
        :return: Unique id assigned to task
        """

    @property
    @abstractmethod
    def requires(self) -> List[str]:
        """ List of tasks whose outputs are used in this task.

        :return: List of Task child classes
        """

    @property
    @abstractmethod
    def depends(self) -> List[DependencyInput]:
        """ List of programs to run to generate intermediary output for this task

        :return: List of Task child classes
        """

    @abstractmethod
    def run(self):
        """ Implementation of method to run to complete a given task

        :return:
        """
