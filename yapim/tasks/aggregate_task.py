from abc import ABC, abstractmethod
from typing import KeysView, ValuesView, ItemsView

from yapim.tasks.task import Task
from yapim.tasks.utils.input_dict import InputDict
from yapim.tasks.utils.task_result import TaskResult
from yapim.utils.config_manager import ConfigManager


class AggregateTask(Task, ABC):
    def __init__(self,
                 record_id: str,
                 task_scope: str,
                 config_manager: ConfigManager,
                 input_data: dict,
                 wdir: str,
                 display_messages: bool):
        super().__init__(record_id, task_scope, config_manager, input_data, {}, wdir, display_messages)
        self.input = InputDict(input_data)
        self.remap_results = False

    def input_ids(self) -> KeysView:
        return self.input.keys()

    def input_values(self) -> ValuesView:
        return self.input.values()

    def input_items(self) -> ItemsView:
        return self.input.items()

    def remap(self):
        self.remap_results = True

    @abstractmethod
    def deaggregate(self) -> dict:
        """

        :return:
        :rtype:
        """

    @staticmethod
    def finalize(obj_results: dict, class_results: dict, task: "AggregateTask", result: TaskResult) -> dict:
        output = task.deaggregate()
        if task.remap_results:
            return output
        if not isinstance(output, dict):
            output = task.output
        else:
            output.update(result)
        keys = set(output.keys())
        for key, value in output.items():
            if key in class_results.keys():
                class_results[key][result.task_name] = value
        to_remove = []
        for key in class_results.keys():
            if key not in keys:
                to_remove.append(key)
        for key in to_remove:
            del class_results[key]
        class_results[result.task_name] = result
        return class_results
