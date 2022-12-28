"""Manage execution of Task pipeline across input set"""

import logging
import os
import pickle
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Union

from art import tprint
# pylint: disable=no-member
from plumbum import colors

from yapim import AggregateTask
from yapim.tasks.task_chain_distributor import TaskChainDistributor
from yapim.utils.config_manager import ConfigManager
from yapim.utils.dependency_graph import Node, DependencyGraph
from yapim.utils.input_loader import InputLoader
from yapim.utils.package_management.package_loader import PackageLoader
from yapim.utils.path_manager import PathManager


class Executor:
    """YAPIM executor generates a topologically-sorted list of Tasks to complete. AggregateTasks break TaskLists -
    execution of Task list ends at an AggregateTask and waits for complete input to reach this point before
    proceeding"""
    def __init__(self,
                 input_data: InputLoader,
                 config_path: Union[Path, str],
                 base_output_dir: Union[Path, str],
                 pipeline_steps_directory: Union[Path, str],
                 dependencies_directories: Optional[List[Union[Path, str]]] = None,
                 display_status_messages: bool = True
                 ):
        """ Generate executor

        :param input_data: Extends InputLoader, this object's abstract methods will be called to generate initial input
         data
        :param config_path: Path to configuration file for pipeline
        :param base_output_dir: Output directory to which to write
        :param pipeline_steps_directory: Directory (possibly nested) of Tasks/AggregateTasks in pipeline
        :param dependencies_directories: Directory (possibly nested) of dependencies in pipeline. Names may overwrite
         existing pipeline steps
        :param display_status_messages: Display status messages as pipeline runs
        """
        pipeline_tasks, self.task_blueprints = PackageLoader.load_from_directories(pipeline_steps_directory,
                                                                                   dependencies_directories)
        self.task_list: List[List[Node]] = DependencyGraph(pipeline_tasks, self.task_blueprints) \
            .sorted_graph_identifiers

        self.pipeline_name = os.path.basename(pipeline_steps_directory)
        self.path_manager = PathManager(base_output_dir)
        self.results_base_dir = base_output_dir.joinpath(PathManager.RESULTS).joinpath(self.pipeline_name)
        if not self.results_base_dir.exists():
            os.makedirs(self.results_base_dir)
        print(colors.yellow & colors.bold | "Gathering files...")
        print(colors.yellow & colors.bold | "------------------")
        self.input_data_dict = Executor._load_input_data(input_data)
        self.config_manager = None
        self.display_messages = display_status_messages
        try:
            self.config_manager = ConfigManager(config_path, input_data.storage_directory())
        # pylint: disable=broad-except
        except BaseException as err:
            print(err)
            sys.exit(1)
        TaskChainDistributor.initialize_class()
        TaskChainDistributor.set_allocations(self.config_manager)
        TaskChainDistributor.results.update(self.input_data_dict)
        TaskChainDistributor.output_data_to_pickle.update({key: {} for key in TaskChainDistributor.results.keys()})
        existing_data = InputLoader.populate_requested_existing_input(
            self.config_manager.config[ConfigManager.INPUT], self.results_base_dir)
        for key, value in existing_data.items():
            if key not in self.input_data_dict.keys():
                self.input_data_dict[key] = {}
            if key not in TaskChainDistributor.results.keys():
                TaskChainDistributor.results[key] = {}
            self.input_data_dict[key].update(value)
            TaskChainDistributor.results[key].update(value)
        self.begin_logging(base_output_dir)

    @staticmethod
    def _load_input_data(input_data: InputLoader):
        """Call InputLoader load method"""
        input_data_dict: dict = input_data.load()
        for key in input_data_dict.keys():
            key = str(key)
            if " " in key:
                raise AttributeError("Valid input key types must not contain spaces in their names")
            if "object at" in key:
                raise AttributeError("Valid input key types must implement .__str__(self) that returns unique ids")
        return input_data_dict

    def begin_logging(self, base_output_dir: Path):
        """Log pipeline top-level messages"""
        log_file = os.path.join(base_output_dir, "%s-eukmetasanity.log" % self.pipeline_name)
        logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO, filename=log_file, filemode='a')
        for _line in ("*" * 80, "",
                      "Primary log statements are redirected to %s" % log_file,
                      "Task-level log statements are redirected to subdirectory log files", "",
                      "*" * 80, "",
                      "Displaying step summaries here:\n"):
            print(colors.yellow & colors.bold | _line)

    def run(self):
        """Launch executor!"""
        if len(self.input_data_dict) == 0:
            print(colors.red & colors.bold | "No input was provided, exiting")
            sys.exit()
        tprint(self.pipeline_name, font="smslant")
        for task_batch in self._task_batch():
            workers = self._get_max_resources_in_batch(task_batch[1])
            with ThreadPoolExecutor(workers) as executor:
                futures = []
                if len(task_batch[1]) == 0:
                    continue
                if task_batch[0] == "Task":
                    for record_id, input_data in TaskChainDistributor.results.items():
                        if record_id in self.task_blueprints.keys():
                            continue
                        task_chain = TaskChainDistributor(record_id, task_batch[1], self.task_blueprints,
                                                          self.config_manager, self.path_manager, input_data,
                                                          self.results_base_dir, self.display_messages)
                        futures.append(executor.submit(task_chain.run))
                else:
                    if len(TaskChainDistributor.results.keys()) == 0:
                        continue
                    first_item = list(TaskChainDistributor.results.keys())[0]
                    task_chain = TaskChainDistributor(first_item, task_batch[1], self.task_blueprints,
                                                      self.config_manager, self.path_manager,
                                                      TaskChainDistributor.results,
                                                      self.results_base_dir, self.display_messages)
                    futures.append(executor.submit(task_chain.run))
                for future in as_completed(futures):
                    exception = future.exception()
                    if exception is not None:
                        raise exception
        with open(self.results_base_dir.joinpath(f"{self.pipeline_name}.pkl"), "wb") as out_ptr:
            pickle.dump(TaskChainDistributor.output_data_to_pickle, out_ptr)
        print(colors.yellow & colors.bold | "\n%s complete!\n" % self.pipeline_name)

    def _task_batch(self):
        """Batch tasks based on AggregateTasks in pipeline"""
        agg_positions = []
        task: Node
        for i, task_list in enumerate(self.task_list):
            for task in task_list:
                if issubclass(self.task_blueprints[task.name], AggregateTask):
                    agg_positions.append(i)
                    break
        start = 0
        for pos in agg_positions:
            yield "Task", self.task_list[start: pos]
            yield "Agg", [self.task_list[pos]]
            start = pos + 1
        yield "Task", self.task_list[start:]

    def _get_max_resources_in_batch(self, task_batch: List[List[Node]]) -> int:
        """Determine resource allotments based on threads and memory and return limiting resource"""
        min_threads: int = 500
        min_memory: int = 50000
        for task_list in task_batch:
            for task in task_list:
                threads = self.config_manager.find(task.get(), ConfigManager.THREADS)
                if threads is None:
                    threads = min_threads
                min_threads = min(min_threads, threads)
                memory = self.config_manager.find(task.get(), ConfigManager.MEMORY)
                if memory is None:
                    memory = min_memory
                min_memory = min(min_memory, memory)
        min_threads = self.config_manager.config[ConfigManager.GLOBAL][ConfigManager.MAX_THREADS] // min_threads or 1
        min_memory = self.config_manager.config[ConfigManager.GLOBAL][ConfigManager.MAX_MEMORY] // min_memory or 1
        if min_memory < min_threads:
            resources = min_memory
        else:
            resources = min_threads
        if resources > 64:
            return 64
        return resources
