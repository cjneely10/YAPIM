from abc import ABC, abstractmethod

from src.tasks.task import Task


class AggregateTask(Task, ABC):
    def __init__(self, record_id: str, task_scope: str, result_map, wdir: str):
        super().__init__(record_id, task_scope, result_map, wdir)
        self.input_data: dict = result_map
        self.input.update(self.aggregate())

    @abstractmethod
    def aggregate(self) -> dict:
        """

        :return:
        :rtype:
        """
