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
from HPCBioPipe.utils.dependency_graph import Node, DependencyGraph
from HPCBioPipe.utils.path_manager import PathManager


class TaskChainDistributor(dict):
    awaiting_aggregate_okay: threading.Condition = threading.Condition()
    secondary_aggregate_waiting: threading.Condition = threading.Condition()
    awaiting_resources: threading.Condition = threading.Condition()
    update_lock: threading.Lock = threading.Lock()

    results: dict
    output_data_to_pickle: dict
    task_reference_count: int
    current_threads_in_use_count: int
    current_gb_memory_in_use_count: int
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
    def initialize_class():
        TaskChainDistributor.results = {}
        TaskChainDistributor.output_data_to_pickle = {}
        TaskChainDistributor.task_reference_count = 0
        TaskChainDistributor.current_threads_in_use_count = 0
        TaskChainDistributor.current_gb_memory_in_use_count = 0

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

        wdir = ".".join(task_identifier.get()).replace(f"{ConfigManager.ROOT}.", "")
        task_blueprint = self.task_blueprints[task_identifier.name]
        task = None
        if TaskChainDistributor._is_aggregate(task_blueprint):
            if not task_blueprint.is_running:
                self.path_manager.add_dirs(wdir)
                with TaskChainDistributor.update_lock:
                    task_blueprint.is_running = True
            else:
                with TaskChainDistributor.secondary_aggregate_waiting:
                    TaskChainDistributor.secondary_aggregate_waiting.wait()
                self[task_blueprint.__name__] = TaskChainDistributor.results[self.record_id][task_blueprint.__name__]
                # print(self)
                return
        else:
            with TaskChainDistributor.update_lock:
                TaskChainDistributor.task_reference_count += 1
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

        if TaskChainDistributor._is_aggregate(self.task_blueprints[task_identifier.name]):
            with TaskChainDistributor.awaiting_aggregate_okay:
                while TaskChainDistributor.task_reference_count != 0:
                    TaskChainDistributor.awaiting_aggregate_okay.wait()
            task = task_blueprint(
                wdir,
                ConfigManager.ROOT,
                self.config_manager,
                TaskChainDistributor.results,
                self.path_manager.get_dir(wdir),
                self.display_status_messages
            )

        task.set_is_complete()

        projected_memory = int(self.config_manager.find(task.full_name, ConfigManager.MEMORY))
        projected_threads = int(self.config_manager.find(task.full_name, ConfigManager.THREADS))
        total_memory = projected_memory + TaskChainDistributor.current_gb_memory_in_use_count
        total_threads = projected_threads + TaskChainDistributor.current_threads_in_use_count
        with TaskChainDistributor.awaiting_resources:
            while total_threads > TaskChainDistributor.maximum_threads or \
                    total_memory > TaskChainDistributor.maximum_gb_memory:
                TaskChainDistributor.awaiting_resources.wait()
                total_memory = projected_memory + TaskChainDistributor.current_gb_memory_in_use_count
                total_threads = projected_threads + TaskChainDistributor.current_threads_in_use_count
        with TaskChainDistributor.update_lock:
            TaskChainDistributor.current_threads_in_use_count = total_threads
            TaskChainDistributor.current_gb_memory_in_use_count = total_memory

        future = TaskChainDistributor.executor.submit(task.run_task)
        possible_failure = future.exception()
        if future.exception() is not None:
            if isinstance(task, AggregateTask):
                with TaskChainDistributor.update_lock:
                    type(task).is_running = False
                with TaskChainDistributor.secondary_aggregate_waiting:
                    TaskChainDistributor.secondary_aggregate_waiting.notifyAll()
            TaskChainDistributor._release_resources(projected_threads, projected_memory)
            raise possible_failure
        result: Result = future.result()

        TaskChainDistributor._release_resources(projected_threads, projected_memory)
        self._finalize_output(task, result)

    @staticmethod
    def _release_resources(projected_threads: int, projected_memory: int):
        with TaskChainDistributor.update_lock:
            TaskChainDistributor.task_reference_count -= 1
            TaskChainDistributor.current_threads_in_use_count -= projected_threads
            TaskChainDistributor.current_gb_memory_in_use_count -= projected_memory
        with TaskChainDistributor.awaiting_resources:
            TaskChainDistributor.awaiting_resources.notifyAll()
        if TaskChainDistributor.task_reference_count == 0:
            with TaskChainDistributor.awaiting_aggregate_okay:
                TaskChainDistributor.awaiting_aggregate_okay.notifyAll()

    def _finalize_output(self, task: Task, result: Result):
        if result.record_id not in TaskChainDistributor.results.keys():
            TaskChainDistributor.results[result.record_id] = {}
            TaskChainDistributor.output_data_to_pickle[result.record_id] = {}
        if isinstance(task, AggregateTask):
            del TaskChainDistributor.results[result.record_id]
            output = task.deaggregate()
            if not isinstance(output, dict):
                raise DependencyGraph.ERR
            for key, value in output.items():
                # print(result.task_name, key)
                TaskChainDistributor.results[key][result.task_name] = value
            with TaskChainDistributor.update_lock:
                type(task).is_running = False
            with TaskChainDistributor.secondary_aggregate_waiting:
                TaskChainDistributor.secondary_aggregate_waiting.notifyAll()
        else:
            TaskChainDistributor.results[result.record_id][result.task_name] = result
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
