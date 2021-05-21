import os
import pickle
from pathlib import Path
from typing import List, Dict, Type, Optional

from HPCBioPipe.tasks.task import Task
from HPCBioPipe.tasks.utils.loader import get_modules
from HPCBioPipe.utils.config_manager import ConfigManager, ImproperInputSection
from HPCBioPipe.utils.dependency_graph import Node, DependencyGraph
from HPCBioPipe.utils.input_loader import InputLoader
from HPCBioPipe.utils.path_manager import PathManager
from HPCBioPipe.utils.task_distributor import TaskDistributor


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
        input_data_dict = input_data.load()
        config_manager = ConfigManager(config_path)
        self.result_map: TaskDistributor = TaskDistributor(config_manager, input_data_dict,
                                                           self.results_base_dir, display_status_messages)

    # TODO: Executor method based on either distribute by task (for servers) or by task chain (for HPCs)
    def run(self):
        for task_list in self.task_list:
            if len(task_list) == 1:
                self.result_map.distribute(self.task_blueprints[task_list[0].name], task_list[0], self.path_manager)
            else:
                for task_id in task_list[:-1]:
                    self.result_map.distribute(self.task_blueprints[task_id.name], task_id, self.path_manager,
                                               self.task_blueprints[task_list[-1].name])
                self.result_map.distribute(self.task_blueprints[task_list[-1].name], task_list[-1], self.path_manager)
        out_ptr = open(self.results_base_dir.joinpath(f"{self.pipeline_name}.pkl"), "wb")
        pickle.dump(self.result_map.output_data_to_pickle, out_ptr)
        out_ptr.close()

    def _populate_requested_existing_input(self, config_manager: ConfigManager) -> Dict[str, Dict]:
        input_section = config_manager.config[ConfigManager.INPUT]
        err = ImproperInputSection("INPUT should consist of dictionary {pipeline_name: key-mapping} or "
                                   "{pipeline_name: all}")
        if not isinstance(input_section, dict):
            raise err
        requested_input = {}
        for requested_pipeline_id in input_section.keys():
            pipeline_input = input_section[requested_pipeline_id]
            if requested_pipeline_id == ConfigManager.ROOT:
                continue
            if isinstance(pipeline_input, str):
                requested_input.update(
                    InputLoader.load_pkl_data(self.results_base_dir.joinpath(requested_pipeline_id + ".pkl"))
                )
            elif isinstance(pipeline_input, dict):
                pkl_data = InputLoader.load_pkl_data(self.results_base_dir.joinpath(requested_pipeline_id + ".pkl"))
                pkl_input_data = {key: {} for key in pkl_data.keys()}
                for _from, _to in pipeline_input.items():
                    for record_id, results_dict in pkl_data.keys():
                        pkl_input_data[record_id][_to] = pkl_data[record_id][_from]
                requested_input.update(pkl_input_data)
            else:
                raise err
        return requested_input
