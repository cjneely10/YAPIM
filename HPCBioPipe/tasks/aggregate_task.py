from abc import ABC, abstractmethod

from HPCBioPipe.tasks.task import Task
from HPCBioPipe.tasks.utils.input_dict import ImmutableDict
from HPCBioPipe.utils.config_manager import ConfigManager
from HPCBioPipe.utils.dependency_graph import DependencyGraph


class AggregateTask(Task, ABC):
    is_running = False

    def __init__(self,
                 record_id: str,
                 task_scope: str,
                 config_manager: ConfigManager,
                 input_data: dict,
                 wdir: str,
                 display_messages: bool):
        AggregateTask.is_running = True
        super().__init__(record_id, task_scope, config_manager, input_data, {}, wdir, display_messages)
        self.input = ImmutableDict(input_data)
        result = self.aggregate()
        if not isinstance(result, dict):
            raise DependencyGraph.ERR
        result.update(self.input)
        self.input = ImmutableDict(result)

    @abstractmethod
    def aggregate(self) -> dict:
        """

        :return:
        :rtype:
        """

    @abstractmethod
    def deaggregate(self) -> dict:
        """

        :return:
        :rtype:
        """

    def __eq__(self, other: "AggregateTask"):
        if not isinstance(other, AggregateTask):
            return False
        return self.name == other.name
