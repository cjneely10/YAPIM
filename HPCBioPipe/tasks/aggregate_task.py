from abc import ABC, abstractmethod

from HPCBioPipe.tasks.task import Task
from HPCBioPipe.tasks.utils.input_dict import ImmutableDict
from HPCBioPipe.utils.dependency_graph import DependencyGraph


class AggregateTask(Task, ABC):
    def __init__(self, record_id: str, task_scope: str, result_map, wdir: str, display_messages: bool):
        super().__init__(record_id, task_scope, result_map, {}, wdir, display_messages)
        self.input: ImmutableDict = ImmutableDict(result_map)
        result = self.aggregate()
        if not isinstance(result, dict):
            raise DependencyGraph.ERR
        self.input = ImmutableDict(result)

    @abstractmethod
    def aggregate(self) -> dict:
        """

        :return:
        :rtype:
        """

    def __eq__(self, other: "AggregateTask"):
        if not isinstance(other, AggregateTask):
            return False
        return self.name == other.name
