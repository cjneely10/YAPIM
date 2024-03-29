"""Clean pipeline output contents by record id or by task name"""
import glob
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from typing import List

from yapim.utils.dependency_graph import DependencyGraph
from yapim.utils.package_management.package_loader import PackageLoader
from yapim.utils.path_manager import PathManager


class DirectoryCleaner:
    """Manages working directory for a pipeline. Clean old data and remove ids from the analysis."""
    def __init__(self, output_directory: Path):
        """
        Create cleaner

        :param output_directory: Pipeline output top-level directory
        """
        if not output_directory.exists():
            print(f"{str(output_directory)} does not exist")
            sys.exit(1)
        self.output_directory = output_directory
        self.record_ids = set(os.listdir(self.output_directory.joinpath(PathManager.WDIR)))

    @staticmethod
    def _rm_glob(file_path: Path):
        for file in glob.glob(str(file_path)):
            if os.path.exists(file):
                if os.path.isdir(file):
                    shutil.rmtree(file)
                else:
                    os.remove(file)

    def clean(self, pipeline_directory: Path, task_names: List[str]):
        """
        Remove task information by task name

        :param pipeline_directory: Pipeline code directory
        :param task_names: List of tasks to delete
        """
        pipeline_tasks, task_blueprints = PackageLoader(pipeline_directory).load_from_package()
        with ThreadPoolExecutor() as executor:
            futures = []
            for task_name in task_names:
                _task_names = DependencyGraph(pipeline_tasks, task_blueprints).get_affected_nodes(task_name)
                for _name in _task_names:
                    print(f"Removing {_name}")
                    task_path = self.output_directory.joinpath(PathManager.WDIR).joinpath("*").joinpath(_name + "*")
                    futures.append(executor.submit(DirectoryCleaner._rm_glob, task_path))
            wait(futures)

    def remove(self, record_ids: List[str]):
        """
        Remove record information by record id

        :param record_ids: Record ids to remove
        """
        with ThreadPoolExecutor() as executor:
            futures = []
            for record_id in record_ids:
                record_id = str(record_id)
                if record_id in self.record_ids:
                    print(f"Removing {record_id}")
                    # Remove wdir contents
                    task_path = self.output_directory.joinpath(PathManager.WDIR).joinpath(record_id)
                    futures.append(executor.submit(DirectoryCleaner._rm_glob, task_path))
                    # Remove results contents
                    task_path = self.output_directory.joinpath(PathManager.RESULTS).joinpath("*").joinpath(record_id)
                    futures.append(executor.submit(DirectoryCleaner._rm_glob, task_path))
                    # Remove input file
                    task_path = self.output_directory.joinpath(PathManager.STORAGE_DIR).joinpath(record_id + "*")
                    futures.append(executor.submit(DirectoryCleaner._rm_glob, task_path))
            wait(futures)
