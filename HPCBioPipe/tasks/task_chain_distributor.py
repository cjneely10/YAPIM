import os
import threading
from concurrent.futures import ThreadPoolExecutor, Executor, Future
from pathlib import Path
from shutil import copy
from typing import List, Type, Optional, Dict, Union

from HPCBioPipe import Task, AggregateTask
from HPCBioPipe.tasks.task import TaskSetupError
from HPCBioPipe.tasks.utils.result import Result
from HPCBioPipe.utils.config_manager import ConfigManager
from HPCBioPipe.utils.dependency_graph import Node
from HPCBioPipe.utils.path_manager import PathManager


class TaskChainDistributor(dict):
    awaiting_aggregate_okay: threading.Condition = threading.Condition()
    awaiting_resources: threading.Condition = threading.Condition()
    update_lock: threading.Lock = threading.RLock()

    results: dict = {}
    output_data_to_pickle: dict = {}
    task_reference_count: int = 0
    current_threads_in_use_count: int = 0
    current_gb_memory_in_use_count: int = 0
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
        self.record_id = record_id
        self.task_identifiers: List[List[Node]] = task_identifiers
        self.task_blueprints: Dict[str, Type[Union[Task, AggregateTask]]] = task_blueprints
        self.config_manager = config_manager
        self.path_manager = path_manager
        self.results_dir = results_base_dir
        self.output_data_to_pickle = {key: {} for key in input_data.keys()}
        self.display_status_messages = display_status_messages

    @staticmethod
    def set_allocations(config_manager: ConfigManager):
        TaskChainDistributor.maximum_threads = int(
            config_manager.config[ConfigManager.GLOBAL][ConfigManager.MAX_THREADS])
        TaskChainDistributor.maximum_gb_memory = int(
            config_manager.config[ConfigManager.GLOBAL][ConfigManager.MAX_MEMORY])

    def run(self):
        for task_ids in self.task_identifiers:
            if len(task_ids) == 1:
                self._run_task(task_ids[0])
            else:
                for task_id in task_ids[:-1]:
                    self._run_task(task_id, task_ids[-1])
                self._run_task(task_ids[-1])

    @staticmethod
    def _is_aggregate(task: Type[Task]):
        return issubclass(task, AggregateTask)

    def _run_task(self, task_identifier: Node, top_level_node: Optional[Node] = None):
        if TaskChainDistributor._is_aggregate(self.task_blueprints[task_identifier.name]):
            with TaskChainDistributor.awaiting_aggregate_okay:
                while TaskChainDistributor.task_reference_count > 0:
                    TaskChainDistributor.awaiting_aggregate_okay.wait()
                TaskChainDistributor.awaiting_aggregate_okay.notifyAll()
        else:
            with TaskChainDistributor.update_lock:
                TaskChainDistributor.task_reference_count += 1

        wdir = ".".join(task_identifier.get()).replace(f"{ConfigManager.ROOT}.", "")
        task_blueprint = self.task_blueprints[task_identifier.name]
        if TaskChainDistributor._is_aggregate(task_blueprint):
            self.path_manager.add_dirs(wdir)
            task = task_blueprint(
                wdir,
                ConfigManager.ROOT,
                self.config_manager,
                TaskChainDistributor.results,
                self.path_manager.get_dir(wdir),
                self.display_status_messages
            )
        else:
            updated_data = {}
            if top_level_node is not None:
                updated_data = self._update_distributed_input(self.record_id, self.task_blueprints[top_level_node.name])
            self.path_manager.add_dirs(self.record_id, [wdir])
            task = task_blueprint(
                self.record_id,
                (ConfigManager.ROOT if top_level_node is None else top_level_node.name),
                self.config_manager,
                self,
                updated_data,
                self.path_manager.get_dir(self.record_id, wdir),
                self.display_status_messages
            )
        task.set_is_complete()

        projected_memory = int(self.config_manager.find(task.full_name, ConfigManager.MEMORY))
        projected_threads = int(self.config_manager.find(task.full_name, ConfigManager.THREADS))
        with TaskChainDistributor.awaiting_resources:
            total_memory = projected_memory + TaskChainDistributor.current_gb_memory_in_use_count
            total_threads = projected_threads + TaskChainDistributor.current_threads_in_use_count
            while total_threads > TaskChainDistributor.maximum_threads or \
                    total_memory > TaskChainDistributor.maximum_gb_memory:
                TaskChainDistributor.awaiting_resources.wait()
                total_memory = projected_memory + TaskChainDistributor.current_gb_memory_in_use_count
                total_threads = projected_threads + TaskChainDistributor.current_threads_in_use_count
            TaskChainDistributor.awaiting_resources.notifyAll()
        with TaskChainDistributor.update_lock:
            TaskChainDistributor.current_threads_in_use_count += projected_threads
            TaskChainDistributor.current_gb_memory_in_use_count += projected_memory

        future = TaskChainDistributor.executor.submit(task.run_task)
        self._finalize_output(task, future)

        with TaskChainDistributor.update_lock:
            if not isinstance(task, AggregateTask):
                TaskChainDistributor.task_reference_count -= 1
            TaskChainDistributor.current_threads_in_use_count -= projected_threads
            TaskChainDistributor.current_gb_memory_in_use_count -= projected_memory

    def _finalize_output(self, task: Task, future: Future):
        result: Result = future.result()
        if result.record_id not in TaskChainDistributor.results.keys():
            with TaskChainDistributor.update_lock:
                TaskChainDistributor.results[result.record_id] = {}
                TaskChainDistributor.output_data_to_pickle[result.record_id] = {}
        with TaskChainDistributor.update_lock:
            TaskChainDistributor.results[result.record_id][result.task_name] = result
            if isinstance(task, AggregateTask):
                output = task.deaggregate()
                for key, value in output.items():
                    TaskChainDistributor.results[key][result.task_name] = value
                self[result.task_name] = output[self.record_id]
            else:
                self[result.task_name] = result
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
        print(task.name)

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
