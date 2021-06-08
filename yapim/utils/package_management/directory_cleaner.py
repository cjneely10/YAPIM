import glob
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from typing import List, Optional, Type, Dict

from yapim import Task
from yapim.tasks.utils.loader import get_modules
from yapim.utils.dependency_graph import DependencyGraph
from yapim.utils.package_management.package_loader import PackageLoader
from yapim.utils.path_manager import PathManager


class DirectoryCleaner:
    def __init__(self, output_directory: Path):
        if not output_directory.exists():
            print(f"{str(output_directory)} does not exist")
            exit(1)
        self.output_directory = output_directory
        self.record_ids = set(os.listdir(self.output_directory.joinpath(PathManager.WDIR)))

    @staticmethod
    def _rm_glob(file_list: List[str]):
        for file in file_list:
            if os.path.exists(file):
                shutil.rmtree(file)

    def clean(self, pipeline_directory: Path, task_name: str, dependency_name: Optional[str] = None):
        pipeline_tasks, task_blueprints = PackageLoader(pipeline_directory).load_from_package()
        task_names = DependencyGraph(pipeline_tasks, task_blueprints).get_affected_nodes(task_name, dependency_name)
        with ThreadPoolExecutor() as executor:
            futures = []
            for _task_name in task_names:
                task_path = self.output_directory.joinpath(PathManager.WDIR).joinpath("*").joinpath(_task_name)
                futures.append(executor.submit(DirectoryCleaner._rm_glob, glob.glob(str(task_path))))
            wait(futures)

    def remove(self, record_ids: List[str]):
        with ThreadPoolExecutor() as executor:
            futures = []
            for record_id in record_ids:
                if record_id in self.record_ids:
                    task_path = self.output_directory.joinpath(PathManager.WDIR).joinpath(record_id)
                    futures.append(executor.submit(DirectoryCleaner._rm_glob, glob.glob(str(task_path))))
            wait(futures)
