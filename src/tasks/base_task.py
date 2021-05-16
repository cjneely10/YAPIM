from abc import ABC, abstractmethod
from typing import List, Optional

from src.utils.result import Result


class BaseTask(ABC):
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
    def requires(self) -> List["BaseTask"]:
        """ List of tasks whose outputs are used in this task.

        :return: List of Task child classes
        """

    @property
    @abstractmethod
    def depends(self) -> Optional[List["BaseTask"]]:
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
        return Result(self.record_id, self.task_name, self.output)
