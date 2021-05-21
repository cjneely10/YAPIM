import os
import threading
from concurrent.futures import ThreadPoolExecutor, Executor, Future
from shutil import copy
from pathlib import Path
from typing import List, Type, Optional, Dict

from HPCBioPipe import Task, AggregateTask
from HPCBioPipe.tasks.task import TaskSetupError
from HPCBioPipe.tasks.utils.result import Result
from HPCBioPipe.utils.config_manager import ConfigManager
from HPCBioPipe.utils.dependency_graph import Node
from HPCBioPipe.utils.path_manager import PathManager


class TaskChainDistributor(dict):
    in_task: threading.Condition = threading.Condition()
    update_lock: threading.Lock = threading.RLock()
    task_count_lock: threading.Lock = threading.RLock()
    aggregate_task_in_wait: Optional[AggregateTask] = None

    results: dict = {}
    output_data_to_pickle: dict = {}
    task_reference_count: int = 0
    current_threads_in_use_count: int = 0
    total_gb_memory_in_use_count: int = 0
    executor: Executor = ThreadPoolExecutor()

    maximum_threads: int
    maximum_gb_memory: int

    def __init__(self,
                 record_id: str,
                 task_identifiers: List[List[Node]],
                 task_blueprints: Dict[str, Type[Task]],
                 config_manager: ConfigManager,
                 path_manager: PathManager,
                 input_data: Dict,
                 results_base_dir: Path,
                 display_status_messages: bool):
        if TaskChainDistributor.maximum_gb_memory is None or TaskChainDistributor.maximum_threads is None:
            raise AttributeError("Allocations for task chain not set!")
        super().__init__(input_data)
        self.pos = -1
        self.record_id = record_id
        self.task_identifiers: List[List[Node]] = task_identifiers
        self.task_blueprints: Dict[str, Type[Task]] = task_blueprints
        self.config_manager = config_manager
        self.path_manager = path_manager
        self.results_dir = results_base_dir
        self.output_data_to_pickle = {key: {} for key in input_data.keys()}
        self.display_status_messages = display_status_messages

    @staticmethod
    def set_allocations(config_manager: ConfigManager):
        TaskChainDistributor.maximum_threads = int(config_manager.config[ConfigManager.GLOBAL][ConfigManager.THREADS])
        TaskChainDistributor.maximum_gb_memory = int(
            config_manager.config[ConfigManager.GLOBAL][ConfigManager.MAX_MEMORY]
        )

    def __iter__(self):
        return self

    def __next__(self):
        self.pos += 1
        task_ids = self.task_identifiers[self.pos]
        if len(task_ids) == 1:
            self._run_task(task_ids[0])
        else:
            for task_id in task_ids[:-1]:
                self._run_task(task_id, task_ids[-1])
            self._run_task(task_ids[-1])

    def _run_task(self, task_identifier: Node, top_level_node: Optional[Node] = None):
        wdir = ".".join(task_identifier.get()).replace(f"{ConfigManager.ROOT}.", "")
        self.path_manager.add_dirs(self.record_id, [wdir])
        updated_data = {}
        if top_level_node is not None:
            updated_data = self._update_distributed_input(self.record_id, self.task_blueprints[top_level_node.name])
        task = self.task_blueprints[task_identifier.name](
            self.record_id,
            ConfigManager.ROOT,
            TaskChainDistributor.results[self.record_id],
            updated_data,
            self.path_manager.get_dir(self.record_id, wdir),
            self.display_status_messages
        )
        task.set_is_complete()
        future = TaskChainDistributor.executor.submit(task.run_task)
        self._finalize_output(future)

    def _finalize_output(self, future: Future):
        result: Result = future.result()
        if result.record_id not in TaskChainDistributor.results.keys():
            with TaskChainDistributor.update_lock:
                TaskChainDistributor.results[result.record_id] = {}
                TaskChainDistributor.output_data_to_pickle[result.record_id] = {}
        with TaskChainDistributor.update_lock:
            TaskChainDistributor.results[result.record_id][result.task_name] = result
        for result_key, result_data in result.items():
            if result_key == "final":
                if not isinstance(result_data, list):
                    raise TaskSetupError("'final' section of output should be a list of keys")
                _sub_out = os.path.join(self.results_dir, result.record_id)
                if not os.path.exists(_sub_out):
                    os.makedirs(_sub_out)
                for file_str in result_data:
                    obj = result.get(file_str)
                    if obj is None:
                        raise TaskSetupError("'final' should consist of keys present in task output")
                    if isinstance(obj, Path) or (isinstance(obj, str) and os.path.exists(obj)):
                        copy(obj, _sub_out)
                    with TaskChainDistributor.update_lock:
                        TaskChainDistributor.output_data_to_pickle[result.record_id][file_str] = obj

    @staticmethod
    def _update_distributed_input(record_id: str, requirement_node: Type[Task]) -> Dict:
        amended_dict = {}
        for dependency in requirement_node.depends():
            if dependency.collect_by is not None:
                for prior_id, prior_mapping in dependency.collect_by.items():
                    for _from, _to in prior_mapping.items():
                        amended_dict[_to] = TaskChainDistributor.results[record_id][prior_id][_from]
            else:
                amended_dict.update(TaskChainDistributor.results[record_id])
        return amended_dict
