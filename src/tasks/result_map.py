import os
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from pathlib import Path
from shutil import copy
from typing import List, Type

from src.tasks.task import Task
from src.tasks.utils.dependency_graph import Node
from src.tasks.utils.result import Result
from src.utils.config_manager import ConfigManager
from src.utils.path_manager import PathManager


class ResultMap(dict):
    def __init__(self, config_manager: ConfigManager, input_data: dict, results_base_dir: Path):
        super().__init__(input_data)
        self.config_manager = config_manager
        self.results_dir = results_base_dir
        self.output_data_to_pickle = {key: {} for key in input_data.keys()}

    def distribute(self, task: Type[Task], task_identifier: Node, path_manager: PathManager):
        """

        :param task:
        :type task:
        :param task_identifier:
        :type task_identifier:
        :param path_manager:
        :type path_manager:
        :return:
        :rtype:
        """
        workers = self.config_manager.parent_info(task_identifier.get())[ConfigManager.WORKERS]
        with ThreadPoolExecutor(workers) as executor:
            futures: List[Future] = []
            for record_id, record_data in self.items():
                wdir = ".".join(task_identifier.get()).replace(f"{ConfigManager.ROOT}.", "")
                path_manager.add_dirs(record_id, [wdir])
                task_copy = task(
                    record_id,
                    task_identifier.scope,
                    self,
                    path_manager.get_dir(record_id, wdir)
                )
                task_copy.input = record_data
                self._update_input(task_copy)
                futures.append(executor.submit(task_copy.run_task))

            self._finalize_output(futures)

    def _update_input(self, task_copy: Task):
        for dependency in task_copy.depends:
            for prior in dependency.collect_all:
                task_copy.input[prior] = self[task_copy.record_id][prior]
            for prior in dependency.collect_by:
                for prior_id, prior_mapping in prior:
                    for _from, _to in prior_mapping.items():
                        task_copy.input[_to] = self[task_copy.record_id][prior_id][_from]

    def _finalize_output(self, futures: List[Future]):
        for future in as_completed(futures):
            result: Result = future.result()
            self[result.record_id][result.task_name] = result
            for result_key, result_data in result.items():
                if result_key == "final":
                    _sub_out = os.path.join(self.results_dir, result.record_id)
                    if not os.path.exists(_sub_out):
                        os.makedirs(_sub_out)
                    for file_str in result_data:
                        obj = result[file_str]
                        if isinstance(obj, Path):
                            copy(obj, _sub_out)
                        self.output_data_to_pickle[result.record_id][file_str] = obj
