from pathlib import Path
from typing import List, Dict, Type

from src.result_map import ResultMap
from src.tasks.task import Task
from src.tasks.utils.dependency_graph import Node, DependencyGraph
from src.utils.config_manager import ConfigManager
from src.utils.loader import get_modules
from src.utils.path_manager import PathManager


class Executor:
    def __init__(self, config_path: str, pipeline_steps_directory: Path, dependencies_directory: Path,
                 path_manager: PathManager):
        self.task_blueprints: Dict[str, Type[Task]] = get_modules(pipeline_steps_directory)
        self.task_list: List[Node] = DependencyGraph(list(self.task_blueprints.values())).sorted_graph_identifiers
        self.task_blueprints.update(get_modules(dependencies_directory))
        self.path_manager = path_manager
        self.result_map: ResultMap = ResultMap(ConfigManager(config_path))

    def run(self):
        """

        :return:
        :rtype:
        """
        for task_id in self.task_list:
            self.result_map.distribute(self.task_blueprints[task_id.name], task_id, self.path_manager)
