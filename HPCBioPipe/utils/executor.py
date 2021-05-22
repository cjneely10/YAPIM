import os
import pickle
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from typing import List, Dict, Type, Optional

from HPCBioPipe.tasks.task import Task
from HPCBioPipe.tasks.task_chain_distributor import TaskChainDistributor
from HPCBioPipe.tasks.utils.loader import get_modules
from HPCBioPipe.utils.config_manager import ConfigManager, ImproperInputSection
from HPCBioPipe.utils.dependency_graph import Node, DependencyGraph
from HPCBioPipe.utils.input_loader import InputLoader
from HPCBioPipe.utils.path_manager import PathManager


class Executor:
    def __init__(self,
                 input_data: InputLoader,
                 config_path: Path,
                 base_output_dir: Path,
                 pipeline_steps_directory: str,
                 dependencies_directories: Optional[List[str]] = None,
                 display_status_messages: bool = True
                 ):
        self.task_blueprints: Dict[str, Type[Task]] = get_modules(pipeline_steps_directory)
        pipeline_tasks = list(self.task_blueprints.values())
        if dependencies_directories is not None:
            for directory in dependencies_directories:
                self.task_blueprints.update(get_modules(directory))
        self.task_list: List[List[Node]] = DependencyGraph(pipeline_tasks, self.task_blueprints) \
            .sorted_graph_identifiers

        self.pipeline_name = os.path.basename(pipeline_steps_directory)
        self.path_manager = PathManager(base_output_dir)
        self.results_base_dir = base_output_dir.joinpath("results").joinpath(self.pipeline_name)
        if not self.results_base_dir.exists():
            os.makedirs(self.results_base_dir)
        self.input_data_dict = input_data.load()
        self.config_manager = None
        self.display_messages = display_status_messages
        try:
            self.config_manager = ConfigManager(config_path)
        # pylint: disable=broad-except
        except BaseException as e:
            print(e)
            exit(1)
        TaskChainDistributor.set_allocations(self.config_manager)
        TaskChainDistributor.results.update(self.input_data_dict)
        TaskChainDistributor.output_data_to_pickle.update({key: {} for key in TaskChainDistributor.results.keys()})
        self.input_data_dict.update(self._populate_requested_existing_input())

    def run(self):
        with ThreadPoolExecutor() as executor:
            futures = []
            for record_id, input_data in self.input_data_dict.items():
                task_chain = TaskChainDistributor(record_id, self.task_list, self.task_blueprints,
                                                  self.config_manager, self.path_manager, input_data,
                                                  self.results_base_dir, self.display_messages)
                futures.append(executor.submit(task_chain.run))
            wait(futures)
            for future in futures:
                if future.exception() is not None:
                    raise future.exception()
        out_ptr = open(self.results_base_dir.joinpath(f"{self.pipeline_name}.pkl"), "wb")
        pickle.dump(TaskChainDistributor.output_data_to_pickle, out_ptr)
        out_ptr.close()

    def _populate_requested_existing_input(self) \
            -> Dict[str, Dict]:
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
            if isinstance(pipeline_input, dict):
                pkl_data = InputLoader.load_pkl_data(pkl_file)
                pkl_input_data = {key: {} for key in pkl_data.keys() if key in self.input_data_dict.keys()}
                for _from, _to in pipeline_input.items():
                    for record_id in pkl_input_data.keys():
                        if _from in pkl_data[record_id].keys():
                            pkl_input_data[record_id][_to] = pkl_data[record_id][_from]
                requested_input.update(pkl_input_data)
            else:
                raise err
        return requested_input
