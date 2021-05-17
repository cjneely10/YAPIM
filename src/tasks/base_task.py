from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional

from src.tasks.utils.dependency_input import DependencyInput
from src.utils.result import Result


class BaseTask(ABC):
    class TaskCompletionError(Exception):
        def __init__(self, task_name: str, file: Path):
            super().__init__(f"Task {task_name} output file missing {file}")

    @property
    @abstractmethod
    def input(self) -> dict:
        """

        :return:
        :rtype:
        """

    @input.setter
    def input(self, input_data: dict):
        self.input = input_data

    @property
    @abstractmethod
    def output(self) -> dict:
        """

        :return:
        :rtype:
        """

    @output.setter
    def output(self, output_data: dict):
        self.output = output_data

    @property
    @abstractmethod
    def record_id(self) -> str:
        """

        :return:
        :rtype:
        """

    @record_id.setter
    def record_id(self, record_id: str):
        self.record_id = record_id

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
    def depends(self) -> Optional[List[DependencyInput]]:
        """ List of programs to run to generate intermediary output for this task

        :return: List of Task child classes
        """

    @abstractmethod
    def run(self):
        """ Implementation of method to run to complete a given task

        :return:
        """

    def run_task(self) -> Result:
        """ Type of run. For Task objects, this simply calls run(). For other tasks, there
        may be more processing required prior to returning the result.

        This method will be used to return the result of the child Task class implemented run method.

        :return:
        """
        for key, output in self.output:
            if isinstance(output, Path) and not output.exists():
                raise BaseTask.TaskCompletionError(key, output)

        return Result(self.record_id, self.task_name, self.output)
