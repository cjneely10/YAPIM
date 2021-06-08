import glob
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from typing import List

from yapim.utils.path_manager import PathManager


# TODO: Add cleanup for tasks downstream in DAG that may be affected by rm command
class DirectoryCleaner:
    def __init__(self, output_directory: Path):
        if not output_directory.exists():
            print(f"{str(output_directory)} does not exist")
            exit(1)
        self.output_directory = output_directory
        self.record_ids = set(os.listdir(self.output_directory.joinpath(PathManager.WDIR)))

    @staticmethod
    def rm_glob(file_list: List[str]):
        for file in file_list:
            if os.path.exists(file):
                shutil.rmtree(file)

    def _clean(self, task_name: str):
        with ThreadPoolExecutor() as executor:
            futures = []
            for record_id in self.record_ids:
                task_path = self.output_directory.joinpath(PathManager.WDIR).joinpath(record_id).joinpath(task_name)
                futures.append(executor.submit(DirectoryCleaner.rm_glob, glob.glob(str(task_path) + "*")))
            wait(futures)

    def remove(self, record_ids: List[str]):
        with ThreadPoolExecutor() as executor:
            futures = []
            for record_id in record_ids:
                if record_id in self.record_ids:
                    task_path = self.output_directory.joinpath(PathManager.WDIR).joinpath(record_id)
                    futures.append(executor.submit(DirectoryCleaner.rm_glob, glob.glob(str(task_path))))
            wait(futures)
