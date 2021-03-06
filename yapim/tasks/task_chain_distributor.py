"""Group together Tasks to create longer Task chains whose completion is independent of other Task chains"""

import os
import threading
from pathlib import Path
from shutil import copy
from typing import List, Type, Optional, Dict, Union

from yapim import Task, AggregateTask
from yapim.tasks.task import TaskSetupError, TaskExecutionError
from yapim.tasks.utils.task_result import TaskResult
from yapim.utils.config_manager import ConfigManager
from yapim.utils.dependency_graph import Node
from yapim.utils.path_manager import PathManager


class TaskChainDistributor(dict):
    """Run Tasks based on available resources. Populate and track output as completed. Update input to a Task
    prior running."""
    awaiting_resources: threading.Condition = threading.Condition()
    update_lock: threading.Lock = threading.Lock()

    results: dict
    output_data_to_pickle: dict
    current_threads_in_use_count: int
    current_gb_memory_in_use_count: int

    maximum_threads: Optional[int] = None
    maximum_gb_memory: Optional[int] = None

    def __init__(self,
                 record_id: str,
                 task_identifiers: List[List[Node]],
                 task_blueprints: Dict[str, Type[Task]],
                 config_manager: ConfigManager,
                 path_manager: PathManager,
                 input_data: Dict,
                 results_base_dir: Union[Path, str],
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
        """Create empty dictionaries for tracking"""
        TaskChainDistributor.results = {}
        TaskChainDistributor.output_data_to_pickle = {}
        TaskChainDistributor.current_threads_in_use_count = 0
        TaskChainDistributor.current_gb_memory_in_use_count = 0

    @staticmethod
    def set_allocations(config_manager: ConfigManager):
        """Set maximum allocations of threads and memory"""
        TaskChainDistributor.maximum_threads = int(
            config_manager.config[ConfigManager.GLOBAL][ConfigManager.MAX_THREADS])
        TaskChainDistributor.maximum_gb_memory = int(
            config_manager.config[ConfigManager.GLOBAL][ConfigManager.MAX_MEMORY])

    def run(self):
        """Run each task in a task chain"""
        for task_ids in self.task_identifiers:
            if len(task_ids) == 1:
                self._run_task(task_ids[0])
            else:
                for task_id in task_ids[:-1]:
                    self._run_task(task_id, task_ids[-1])
                self._run_task(task_ids[-1])

    @staticmethod
    def _is_aggregate(task: Type[Task]):
        """Task is AggregateTask subclass"""
        return issubclass(task, AggregateTask)

    def _run_task(self, task_identifier: Node, top_level_node: Optional[Node] = None):
        """Run Task/AggregateTask. Wait for available resources prior to launching. Finalize output to output
        directories and provide updated input values prior to launching a Task"""
        wdir = ".".join(task_identifier.get()).replace(f"{ConfigManager.ROOT}.", "")
        task_blueprint = self.task_blueprints[task_identifier.name]
        if TaskChainDistributor._is_aggregate(task_blueprint):
            self.path_manager.add_dirs(wdir)
            task = task_blueprint(
                wdir,
                task_identifier.scope,
                self.config_manager,
                TaskChainDistributor.results,
                self.path_manager.get_dir(wdir),
                self.display_status_messages
            )
        else:
            updated_data = {}
            if top_level_node is not None:
                try:
                    updated_data = self._update_distributed_input(self.record_id,
                                                                  self.task_blueprints[top_level_node.name])
                except KeyError as err:
                    # pylint: disable=raise-missing-from
                    raise TaskExecutionError(f"Unable to load dependency data {err} for {task_identifier.get()} "
                                             f"on record {self.record_id}")
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

        # TODO: This may be problematic...
        projected_memory = int(self.config_manager.find(task.full_name, ConfigManager.MEMORY))
        # TODO: Handle SLURM when multiple nodes may have been listed
        projected_threads = int(self.config_manager.find(task.full_name, ConfigManager.THREADS))
        with TaskChainDistributor.awaiting_resources:
            with TaskChainDistributor.update_lock:
                total_memory = projected_memory + TaskChainDistributor.current_gb_memory_in_use_count
                total_threads = projected_threads + TaskChainDistributor.current_threads_in_use_count
            while total_threads > TaskChainDistributor.maximum_threads or \
                    total_memory > TaskChainDistributor.maximum_gb_memory:
                TaskChainDistributor.awaiting_resources.wait()
                with TaskChainDistributor.update_lock:
                    total_memory = projected_memory + TaskChainDistributor.current_gb_memory_in_use_count
                    total_threads = projected_threads + TaskChainDistributor.current_threads_in_use_count
            with TaskChainDistributor.update_lock:
                TaskChainDistributor.current_threads_in_use_count = total_threads
                TaskChainDistributor.current_gb_memory_in_use_count = total_memory

        try:
            self._finalize_output(task, task.run_task())
            TaskChainDistributor._release_resources(projected_threads, projected_memory)
        except BaseException as err:
            TaskChainDistributor._release_resources(projected_threads, projected_memory)
            raise err

    @staticmethod
    def _release_resources(projected_threads: int, projected_memory: int):
        """Free resources used in running Task"""
        with TaskChainDistributor.update_lock:
            TaskChainDistributor.current_threads_in_use_count -= projected_threads
            TaskChainDistributor.current_gb_memory_in_use_count -= projected_memory
        with TaskChainDistributor.awaiting_resources:
            TaskChainDistributor.awaiting_resources.notifyAll()

    def _finalize_output(self, task: Task, result: TaskResult):
        """Populate Task output to final output directory and output .pkl file. Do not finalize Tasks that were skipped
        """
        with TaskChainDistributor.update_lock:
            if result.record_id not in TaskChainDistributor.results.keys():
                TaskChainDistributor.results[result.record_id] = {}
                # TODO: Manage memory better (write tasks as they complete, reload for AggregateTasks)
                #  https://docs.h5py.org/en/stable/index.html
                TaskChainDistributor.output_data_to_pickle[result.record_id] = {}
        if not isinstance(task, AggregateTask):
            with TaskChainDistributor.update_lock:
                TaskChainDistributor.results = type(task).finalize(self, TaskChainDistributor.results, task, result)
        else:
            TaskChainDistributor.results = type(task).finalize(self, TaskChainDistributor.results, task, result)
        if task.is_skip:
            return
        for result_key, result_data in result.items():
            if result_key == "final":
                if not isinstance(result_data, list):
                    raise TaskSetupError("'final' section of output should be a list of keys")
                _sub_out = os.path.join(self.results_dir, str(result.record_id))
                if not os.path.exists(_sub_out):
                    os.makedirs(_sub_out)
                for file_str in result_data:
                    obj = result.get(file_str)
                    if obj is None:
                        raise TaskSetupError("'final' should consist of keys present in task output")
                    if isinstance(obj, Path) or (isinstance(obj, str) and os.path.exists(obj)):
                        _path = os.path.splitext(os.path.basename(obj))
                        _out = os.path.join(
                            _sub_out,
                            _path[0] + "." + result.task_name + _path[1]
                        )
                        copy(obj, _out)
                        obj = _out
                    with TaskChainDistributor.update_lock:
                        if result.record_id not in TaskChainDistributor.output_data_to_pickle.keys():
                            TaskChainDistributor.output_data_to_pickle[result.record_id] = {}
                        TaskChainDistributor.output_data_to_pickle[result.record_id][file_str] = obj

    @staticmethod
    # pylint: disable=too-many-branches
    def _update_distributed_input(record_id: str, requirement_node: Type[Task]) -> Dict:
        """Populate input to a Task with the requested from:to mapping defined in DependencyInput class"""
        amended_dict = {}
        # pylint: disable=too-many-nested-blocks
        for dependency in requirement_node.depends():
            if dependency.collect_by is not None:
                for prior_id, prior_mapping in dependency.collect_by.items():
                    if isinstance(prior_mapping, dict):
                        if prior_id.lower() != ConfigManager.ROOT.lower():
                            for _from, _to in prior_mapping.items():
                                amended_dict[_to] = TaskChainDistributor.results[record_id][prior_id][_from]
                        else:
                            for _from, _to in prior_mapping.items():
                                amended_dict[_to] = TaskChainDistributor.results[record_id][_from]
                    else:
                        for attr in prior_mapping:
                            if attr.lower() != ConfigManager.ROOT.lower():
                                amended_dict[attr] = TaskChainDistributor.results[record_id][prior_id][attr]
                            else:
                                amended_dict[attr] = TaskChainDistributor.results[record_id][attr]
            else:
                amended_dict.update(TaskChainDistributor.results[record_id])
        return amended_dict
