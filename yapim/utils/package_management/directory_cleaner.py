import glob
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from typing import List, Set, Optional, Dict, Type

from yapim import Task
from yapim.tasks.utils.loader import get_modules
from yapim.utils.dependency_graph import DependencyGraph, Node
from yapim.utils.package_management.package_loader import PackageLoader
from yapim.utils.path_manager import PathManager


# TODO: Add cleanup for tasks downstream in DAG that may be affected by rm command
class DirectoryCleaner:
    def __init__(self, pipeline_directory: Path, output_directory: Path, ):
        if not output_directory.exists():
            print(f"{str(output_directory)} does not exist")
            exit(1)
        self.output_directory = output_directory
        self.record_ids = set(os.listdir(self.output_directory.joinpath(PathManager.WDIR)))
        pipeline_data = PackageLoader(pipeline_directory).validate_pipeline_pkl()
        self.task_blueprints: Dict[str, Type[Task]] = get_modules(pipeline_data["tasks"])
        pipeline_tasks = list(self.task_blueprints.values())
        if pipeline_data["dependencies"] is not None:
            for directory in pipeline_data["dependencies"]:
                self.task_blueprints.update(get_modules(directory))
        self.task_list: List[List[Node]] = DependencyGraph(pipeline_tasks, self.task_blueprints) \
            .sorted_graph_identifiers

    @staticmethod
    def rm_glob(file_list: List[str]):
        for file in file_list:
            if os.path.exists(file):
                shutil.rmtree(file)

    def _gather_dirs_to_remove(self, dirs_to_remove: Set[str], task_name: str, record_ids_to_remove: Set[str]):
        pass
        # for record_id in record_ids_to_remove:
        #     task_path = self.output_directory.joinpath(PathManager.WDIR).joinpath(record_id).joinpath(task_name)
        #     dirs_to_remove.add(glob.glob(str(task_path) + "*"))

    def _clean(self, task_name: str, dependency_name: Optional[str] = None, record_ids: Optional[List[str]] = None):
        pass

    def remove(self, record_ids: List[str]):
        with ThreadPoolExecutor() as executor:
            futures = []
            for record_id in record_ids:
                if record_id in self.record_ids:
                    task_path = self.output_directory.joinpath(PathManager.WDIR).joinpath(record_id)
                    futures.append(executor.submit(DirectoryCleaner.rm_glob, glob.glob(str(task_path))))
            wait(futures)
