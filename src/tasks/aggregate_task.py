from abc import ABC, abstractmethod

from src.tasks.task import Task


class AggregateTask(Task, ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input = self.aggregate()

    @abstractmethod
    def aggregate(self) -> dict:
        """

        :return:
        :rtype:
        """
