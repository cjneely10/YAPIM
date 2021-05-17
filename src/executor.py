from pathlib import Path
from typing import List, Dict, Type, Optional

from src.tasks.result_map import ResultMap
from src.tasks.task import Task
from src.tasks.utils.dependency_graph import Node, DependencyGraph
from src.utils.config_manager import ConfigManager
from src.utils.loader import get_modules
from src.utils.path_manager import PathManager


class Executor:
    def __init__(self,
                 config_path: Path,
                 pipeline_steps_directory: Path,
                 base_dir: Path,
                 input_data: Dict[str, Dict],
                 dependencies_directory: Optional[Path] = None,
                 ):
        self.task_blueprints: Dict[str, Type[Task]] = get_modules(pipeline_steps_directory)
        self.task_list: List[Node] = DependencyGraph(list(self.task_blueprints.values())).sorted_graph_identifiers
        if dependencies_directory is not None:
            self.task_blueprints.update(get_modules(dependencies_directory))
        self.path_manager = PathManager(base_dir)
        self.result_map: ResultMap = ResultMap(ConfigManager(config_path), input_data, base_dir.joinpath("results"))

    def run(self):
        """

        :return:
        :rtype:
        """
        for task_id in self.task_list:
            self.result_map.distribute(self.task_blueprints[task_id.name], task_id, self.path_manager)
