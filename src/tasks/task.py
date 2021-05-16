from abc import ABC, abstractmethod
from typing import List, Optional


class Result(dict):
    """
    Wrapper for python dict class for Result of completing a Task on a given input set
    """
    def __init__(self):
        super().__init__()


class Task(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """

        :return: Unique id assigned to task
        """

    @property
    @abstractmethod
    def requires(self) -> List["Task"]:
        """ List of tasks whose outputs are used in this task.

        :return: List of Task child classes
        """

    @property
    @abstractmethod
    def depends(self) -> Optional[List["Task"]]:
        """ List of programs to run to generate intermediary output for this task

        :return: List of Task child classes
        """

    @abstractmethod
    def run(self) -> Result:
        """ Implementation of method to run to complete a given task

        :return:
        """

    def run_task(self) -> Result:
        """ Type of run. For Task objects, this simply calls run(). For other tasks, there
        may be more processing required prior to returning the result.

        This method will be used to return the result of the child Task class implemented run method.

        :return:
        """
        return self.run()
