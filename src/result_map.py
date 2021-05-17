from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from copy import deepcopy
from typing import List

from src.tasks.task import Task
from src.utils.config_manager import ConfigManager
from src.utils.result import Result


class ResultMap(dict):
    def __init__(self, config_manager: ConfigManager):
        super().__init__()
        self.config_manager = config_manager

    def distribute(self, task: Task):
        """

        :param task:
        :type task:
        :return:
        :rtype:
        """
        with ThreadPoolExecutor(self.config_manager.get(task.full_name)) as executor:
            futures: List[Future] = []
            for record_id, record_data in self.items():
                task_copy = deepcopy(task)
                task_copy.input = record_data
                self._update_input(task_copy)
                futures.append(executor.submit(task.run_task))

            for future in as_completed(futures):
                result: Result = future.result()
                if result.record_id not in self.keys():
                    self[result.record_id] = {}
                self[result.record_id][result.task_name] = result

    def _update_input(self, task_copy: Task):
        for dependency in task_copy.depends:
            for prior in dependency.collect_all:
                task_copy.input[prior] = self[task_copy.record_id][prior]
        for dependency in task_copy.depends:
            for prior in dependency.collect_by:
                for prior_id, prior_mapping in prior:
                    for _from, _to in prior_mapping.items():
                        task_copy.input[_to] = self[task_copy.record_id][prior_id][_from]

