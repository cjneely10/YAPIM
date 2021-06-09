import glob
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from typing import List, Optional

from yapim.utils.config_manager import ConfigManager
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
                if os.path.isdir(file):
                    shutil.rmtree(file)
                else:
                    os.remove(file)

    def clean(self, pipeline_directory: Path, task_name: str):
        pipeline_tasks, task_blueprints = PackageLoader(pipeline_directory).load_from_package()
        task_names = DependencyGraph(pipeline_tasks, task_blueprints).get_affected_nodes(task_name)
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
                record_id = str(record_id)
                if record_id in self.record_ids:
                    # Remove wdir contents
                    task_path = self.output_directory.joinpath(PathManager.WDIR).joinpath(record_id)
                    futures.append(executor.submit(DirectoryCleaner._rm_glob, glob.glob(str(task_path))))
                    # Remove results contents
                    task_path = self.output_directory.joinpath(PathManager.RESULTS).joinpath("*").joinpath(record_id)
                    futures.append(executor.submit(DirectoryCleaner._rm_glob, glob.glob(str(task_path))))
                    # Remove input file
                    task_path = self.output_directory.joinpath(ConfigManager.STORAGE_DIR).joinpath(record_id + "*")
                    futures.append(executor.submit(DirectoryCleaner._rm_glob, glob.glob(str(task_path))))
            wait(futures)
