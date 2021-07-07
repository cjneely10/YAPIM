from abc import ABC, abstractmethod
from typing import KeysView, ValuesView, ItemsView, Optional, Dict, Iterable, Callable, Union

from yapim.tasks.task import Task
from yapim.tasks.utils.input_dict import InputDict
from yapim.tasks.utils.task_result import TaskResult
from yapim.utils.config_manager import ConfigManager


# TODO: Helper filter, update, etc. methods for deaggregate
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
        self._remap_results = False

    def input_ids(self) -> KeysView:  # pragma: no cover
        return self.input.keys()

    def input_values(self) -> ValuesView:  # pragma: no cover
        return self.input.values()

    def input_items(self) -> ItemsView:  # pragma: no cover
        return self.input.items()

    def remap(self):
        self._remap_results = True

    @abstractmethod
    def deaggregate(self) -> Dict[str, Dict]:
        """

        :return:
        :rtype:
        """

    @staticmethod
    def finalize(obj_results: dict, class_results: dict, task: "AggregateTask", result: TaskResult) -> dict:
        # Rule: If deaggregate is not defined, simply collect AggTask result into class_results
        # Rule: If defined and remap results, update deagg results with AggTask results and return
        # Rule: If defined and not remap, update all input items as class_results[record_id][aggtask.name] = deagg(),
        #       remove any ids that are not present, and add any ids that were not originally there
        output = task.deaggregate()
        if output is None:
            class_results[result.task_name] = result
            return class_results
        if task._remap_results:
            output[result.task_name] = task.output
            return output
        class_results[result.task_name] = result
        for key, value in output.items():
            if key not in class_results.keys():
                class_results[key] = {}
            class_results[key][result.task_name] = value
        keys = set(output.keys())
        to_remove = []
        for key in class_results.keys():
            if key not in keys:
                to_remove.append(key)
        for key in to_remove:
            del class_results[key]
        return class_results

    def has_run(self, task_name: str, record_id: Optional[str] = None) -> bool:
        """ Check whether a task was run for a record_id

        :return: Boolean result
        """
        if record_id is None:
            return False
        return task_name in self.input[record_id].keys()

    def filter(self, filter_values: Union[Iterable, Callable[[str], bool]]) -> Dict[str, Dict]:
        if isinstance(filter_values, Iterable):
            return {
                record_id: self.input[record_id]
                for record_id in filter_values
                if record_id in self.input_ids()
            }
        else:
            return {
                record_id: self.input[record_id]
                for record_id in self.input_ids()
                if filter_values(record_id)
            }
