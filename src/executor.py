import os
import pickle
from pathlib import Path
from typing import List, Dict, Type, Optional

from src.tasks.task import Task
from src.tasks.utils.dependency_graph import Node, DependencyGraph
from src.tasks.utils.loader import get_modules
from src.tasks.utils.task_distributor import TaskDistributor
from src.utils.config_manager import ConfigManager
from src.utils.input_loader import InputLoader
from src.utils.path_manager import PathManager


class Executor:
    def __init__(self,
                 input_data: InputLoader,
                 config_path: Path,
                 base_output_dir: Path,
                 pipeline_steps_directory: str,
                 dependencies_directories: Optional[List[str]] = None,
                 ):
        self.task_blueprints: Dict[str, Type[Task]] = get_modules(pipeline_steps_directory)
        pipeline_tasks = list(self.task_blueprints.values())
        if dependencies_directories is not None:
            for directory in dependencies_directories:
                self.task_blueprints.update(get_modules(directory))
        self.task_list: List[List[Node]] = DependencyGraph(pipeline_tasks, self.task_blueprints)\
            .sorted_graph_identifiers

        self.pipeline_name = os.path.basename(pipeline_steps_directory)
        self.path_manager = PathManager(base_output_dir)
        self.results_base_dir = base_output_dir.joinpath("results").joinpath(self.pipeline_name)
        if not self.results_base_dir.exists():
            os.makedirs(self.results_base_dir)
        self.result_map: TaskDistributor = TaskDistributor(ConfigManager(config_path), input_data.load(),
                                                           self.results_base_dir)

    def run(self):
        """

        :return:
        :rtype:
        """
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
