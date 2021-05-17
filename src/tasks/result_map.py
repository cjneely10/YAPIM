import os
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from pathlib import Path
from shutil import copy
from typing import List, Type, Optional

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

    def distribute(self, task: Type[Task], task_identifier: Node, path_manager: PathManager,
                   top_level_node: Optional[Type[Task]] = None):
        """

        :param task:
        :type task:
        :param task_identifier:
        :type task_identifier:
        :param path_manager:
        :type path_manager:
        :param top_level_node:
        :type top_level_node:
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
                if top_level_node is not None:
                    self._update_input(record_id, task_copy, top_level_node)
                futures.append(executor.submit(task_copy.run_task))

            self._finalize_output(futures)

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

    def _update_input(self, record_id: str, task_copy: Task, requirement_node: Type[Task]):
        for dependency in requirement_node.depends:
            for prior_id, prior_mapping in dependency.collect_by.items():
                for _from, _to in prior_mapping.items():
                    task_copy.input[_to] = self[record_id][prior_id][_from]
