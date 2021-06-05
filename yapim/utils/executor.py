import logging
import os
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Dict, Type, Optional, Union

from art import tprint
from plumbum import colors

from yapim import AggregateTask
from yapim.tasks.task import Task
from yapim.tasks.task_chain_distributor import TaskChainDistributor
from yapim.tasks.utils.loader import get_modules
from yapim.utils.config_manager import ConfigManager, ImproperInputSection
from yapim.utils.dependency_graph import Node, DependencyGraph
from yapim.utils.input_loader import InputLoader
from yapim.utils.path_manager import PathManager


class Executor:
    def __init__(self,
                 input_data: InputLoader,
                 config_path: Union[Path, str],
                 base_output_dir: Union[Path, str],
                 pipeline_steps_directory: Union[Path, str],
                 dependencies_directories: Optional[List[Union[Path, str]]] = None,
                 display_status_messages: bool = True
                 ):
        self.task_blueprints: Dict[str, Type[Task]] = get_modules(Path(pipeline_steps_directory))
        pipeline_tasks = list(self.task_blueprints.values())
        if dependencies_directories is not None:
            for directory in dependencies_directories:
                self.task_blueprints.update(get_modules(Path(directory)))
        self.task_list: List[List[Node]] = DependencyGraph(pipeline_tasks, self.task_blueprints) \
            .sorted_graph_identifiers

        self.pipeline_name = os.path.basename(pipeline_steps_directory)
        self.path_manager = PathManager(base_output_dir)
        self.results_base_dir = base_output_dir.joinpath("results").joinpath(self.pipeline_name)
        if not self.results_base_dir.exists():
            os.makedirs(self.results_base_dir)
        print(colors.yellow & colors.bold | "Gathering files...")
        print(colors.yellow & colors.bold | "------------------")
        self.input_data_dict = input_data.load()
        self.config_manager = None
        self.display_messages = display_status_messages
        try:
            self.config_manager = ConfigManager(config_path)
        # pylint: disable=broad-except
        except BaseException as e:
            print(e)
            exit(1)
        TaskChainDistributor.initialize_class()
        TaskChainDistributor.set_allocations(self.config_manager)
        TaskChainDistributor.results.update(self.input_data_dict)
        TaskChainDistributor.output_data_to_pickle.update({key: {} for key in TaskChainDistributor.results.keys()})
        existing_data = self._populate_requested_existing_input()
        self.input_data_dict.update(existing_data)
        TaskChainDistributor.results.update(existing_data)
        self.begin_logging(base_output_dir)

    def begin_logging(self, base_output_dir: Path):
        log_file = os.path.join(base_output_dir, "%s-eukmetasanity.log" % self.pipeline_name)
        logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO, filename=log_file, filemode='a')
        for _line in ("*" * 80, "",
                      "Primary log statements are redirected to %s" % log_file,
                      "Task-level log statements are redirected to subdirectory log files", "",
                      "*" * 80, "",
                      "Displaying step summaries here:\n"):
            print(colors.yellow & colors.bold | _line)

    def run(self):
        tprint(self.pipeline_name, font="smslant")
        for task_batch in self.task_batch():
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
                    first_item = list(TaskChainDistributor.results.keys())[0]
                    task_chain = TaskChainDistributor(first_item, task_batch[1], self.task_blueprints,
                                                      self.config_manager, self.path_manager,
                                                      TaskChainDistributor.results,
                                                      self.results_base_dir, self.display_messages)
                    futures.append(executor.submit(task_chain.run))
                for future in as_completed(futures):
                    if future.exception() is not None:
                        raise future.exception()
        out_ptr = open(self.results_base_dir.joinpath(f"{self.pipeline_name}.pkl"), "wb")
        pickle.dump(TaskChainDistributor.output_data_to_pickle, out_ptr)
        out_ptr.close()
        print(colors.yellow & colors.bold | "\n%s complete!\n" % self.pipeline_name)

    def task_batch(self):
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
        min_threads: int = 500
        min_memory: int = 50000
        for task_list in task_batch:
            for task in task_list:
                min_threads = min(min_threads, self.config_manager.find(task.get(), ConfigManager.THREADS))
                min_memory = min(min_memory, self.config_manager.find(task.get(), ConfigManager.MEMORY))
        min_threads = self.config_manager.config[ConfigManager.GLOBAL][ConfigManager.MAX_THREADS] // min_threads or 1
        min_memory = self.config_manager.config[ConfigManager.GLOBAL][ConfigManager.MAX_MEMORY] // min_memory or 1
        if min_memory < min_threads:
            resources = min_memory
        else:
            resources = min_threads
        if resources > 64:
            return 64
        return resources

    def _populate_requested_existing_input(self) -> Dict[str, Dict]:
        input_section = self.config_manager.config[ConfigManager.INPUT]
        err = ImproperInputSection("INPUT should consist of dictionary {pipeline_name: key-mapping} or "
                                   "{pipeline_name: all}")
        if not isinstance(input_section, dict):
            raise err
        requested_input = {}
        for requested_pipeline_id in input_section.keys():
            pipeline_input = input_section[requested_pipeline_id]
            if requested_pipeline_id == ConfigManager.ROOT:
                continue
            pkl_file = Path(os.path.dirname(self.results_base_dir)) \
                .joinpath(requested_pipeline_id).joinpath(requested_pipeline_id + ".pkl")
            pkl_data = InputLoader.load_pkl_data(pkl_file)
            pkl_input_data = {key: {} for key in pkl_data.keys() if key in self.input_data_dict.keys()}
            if isinstance(pipeline_input, dict):
                for _to, _from in pipeline_input.items():
                    for record_id in pkl_input_data.keys():
                        if _from in pkl_data[record_id].keys():
                            pkl_input_data[record_id][_to] = pkl_data[record_id][_from]
                        else:
                            raise err
                requested_input.update(pkl_input_data)
            else:
                requested_input.update(pkl_data)
        return requested_input
