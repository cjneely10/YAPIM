"""AggregateTask functionality for handling tasks that operate on entire input set at once"""

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import KeysView, ValuesView, ItemsView, Optional, Dict, Callable, Union

from yapim.tasks.task import Task
from yapim.tasks.utils.input_dict import InputDict
from yapim.tasks.utils.task_result import TaskResult
from yapim.utils.config_manager import ConfigManager


class AggregateTask(Task, ABC):
    """An AggregateTask receives the entire input set for the pipeline and can operate on any part of this input"""
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
        """Wrapper for self.input.keys()"""
        return self.input.keys()

    def input_values(self) -> ValuesView:  # pragma: no cover
        """Wrapper for self.input.values()"""
        return self.input.values()

    def input_items(self) -> ItemsView:  # pragma: no cover
        """Wrapper for self.input.items()"""
        return self.input.items()

    def remap(self):
        """ Remove all currently tracked input from storage. `remap()` should be called within `deaggregate()`,
        and the dictionary that this method returns will define the output of the pipeline after this AggregateTask
        completes
        """
        self._remap_results = True

    @abstractmethod
    def deaggregate(self) -> Dict[str, Dict]:
        """ Output result dictionary that will be used to update or populate pipeline input for any downstream
        `Task`/`AggregateTask`.

        Rule: If deaggregate is not defined, the original input data will not be modified.

        Rule: If defined and remap() is called in the method body, currently stored input will be replaced with the
        output of this method.

        Rule: If defined and remap() is not called in the method body, for each id present in the output of this method,
        any labels in the output will be used to update the id's stored results. Ids not present in this dictionary will
        be removed from tracking.

        Input dictionaries resemble:  {record_id: {label: value}}

        :return: Dictionary that will be used to update input data
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
        # pylint: disable=protected-access
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
        """ Check whether a task was run for a record_id. Returns false if no record_id is passed and the caller is
        an instance of `AggregateTask`

        :return: Boolean result
        """
        if record_id is None:
            return False
        return task_name in self.input[record_id].keys()

    def filter(self, filter_values: Union[Iterable, Callable[[object, dict], bool]]) -> Dict[str, Dict]:
        """Filter the stored input data of this pipeline with either:

        1) An iterable of ids that will be kept.

        2) A callable (lambda, function, functor, etc.) that accepts an input record_id and a dictionary of data and
        returns a boolean for if the input will continue to be tracked.
        """
        if isinstance(filter_values, Iterable):
            return {
                record_id: self.input[record_id]
                for record_id in filter_values
                if record_id in self.input_ids()
            }
        return {
            record_id: self.input[record_id]
            for record_id, record_data in self.input_items()
            if filter_values(record_id, record_data)
        }
