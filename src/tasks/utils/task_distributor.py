import os
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from pathlib import Path
from shutil import copy
from typing import List, Type, Optional, Dict

from src.tasks.aggregate_task import AggregateTask
from src.tasks.task import Task
from src.tasks.utils.dependency_graph import Node
from src.tasks.utils.result import Result
from src.utils.config_manager import ConfigManager
from src.utils.path_manager import PathManager


class TaskDistributor(dict):
    def __init__(self, config_manager: ConfigManager, input_data: Dict[str, Dict], results_base_dir: Path,
                 display_status_messages: bool):
        super().__init__(input_data)
        self.config_manager = config_manager
        self.results_dir = results_base_dir
        self.output_data_to_pickle = {key: {} for key in input_data.keys()}
        self.display_status_messages = display_status_messages

    def distribute(self, task: Type[Task], task_identifier: Node, path_manager: PathManager,
                   top_level_node: Optional[Type[Task]] = None):
        workers = self.config_manager.parent_info(task_identifier.get())[ConfigManager.WORKERS]
        with ThreadPoolExecutor(workers) as executor:
            futures: List[Future] = []
            if issubclass(task, AggregateTask):
                self._aggregate_task(task, task_identifier, path_manager, futures, executor)
            else:
                self._distribute_task(task, task_identifier, path_manager, top_level_node, futures, executor)
            self._finalize_output(futures)

    def _distribute_task(self, task: Type[Task], task_identifier: Node, path_manager: PathManager,
                         top_level_node: Optional[Type[Task]], futures: List[Future], executor: ThreadPoolExecutor):
        for record_id in self.keys():
            wdir = ".".join(task_identifier.get()).replace(f"{ConfigManager.ROOT}.", "")
            path_manager.add_dirs(record_id, [wdir])
            task_copy = task(
                record_id,
                task_identifier.scope,
                self,
                path_manager.get_dir(record_id, wdir),
                self.display_status_messages
            )
            task_copy.set_is_complete()
            if top_level_node is not None:
                self._update_input(record_id, task_copy, top_level_node)
            futures.append(executor.submit(task_copy.run_task))

    def _aggregate_task(self, task: Type[AggregateTask], task_identifier: Node, path_manager: PathManager,
                        futures: List[Future], executor: ThreadPoolExecutor):
        wdir = ".".join(task_identifier.get()).replace(f"{ConfigManager.ROOT}.", "")
        path_manager.add_dirs(wdir)
        task_copy = task(
            wdir,
            task_identifier.scope,
            self,
            path_manager.get_dir(wdir),
            self.display_status_messages
        )
        task_copy.set_is_complete()
        futures.append(executor.submit(task_copy.run_task))

    def _finalize_output(self, futures: List[Future]):
        for future in as_completed(futures):
            result: Result = future.result()
            if result.record_id not in self.keys():
                self[result.record_id] = {}
                self.output_data_to_pickle[result.record_id] = {}
            self[result.record_id][result.task_name] = result
            for result_key, result_data in result.items():
                if result_key == "final":
                    _sub_out = os.path.join(self.results_dir, result.record_id)
                    if not os.path.exists(_sub_out):
                        os.makedirs(_sub_out)
                    for file_str in result_data:
                        obj = result[file_str]
                        if isinstance(obj, Path) or (isinstance(obj, str) and os.path.exists(obj)):
                            copy(obj, _sub_out)
                        self.output_data_to_pickle[result.record_id][file_str] = obj

    def _update_input(self, record_id: str, task_copy: Task, requirement_node: Type[Task]):
        for dependency in requirement_node.depends():
            for prior_id, prior_mapping in dependency.collect_by.items():
                for _from, _to in prior_mapping.items():
                    task_copy.dependency_input[_to] = self[record_id][prior_id][_from]
