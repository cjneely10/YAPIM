from pathlib import Path
from typing import Type, Dict, List, Optional

from HPCBioPipe import Task
from HPCBioPipe.tasks.utils.loader import get_modules
from HPCBioPipe.utils.dependency_graph import DependencyGraph, Node


class ConfigManagerGenerator:
    def __init__(self, pipeline_module_path: str, dependencies_directories: Optional[List[str]]):
        self.task_blueprints: Dict[str, Type[Task]] = get_modules(pipeline_module_path)
        pipeline_tasks = list(self.task_blueprints.values())
        if dependencies_directories is not None:
            for directory in dependencies_directories:
                self.task_blueprints.update(get_modules(directory))
        self.task_list: List[List[Node]] = DependencyGraph(pipeline_tasks, self.task_blueprints) \
            .sorted_graph_identifiers

    def write(self, config_file_path: Path):
        pass
