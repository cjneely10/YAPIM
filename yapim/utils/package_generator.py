import os
import pickle
import shutil
from pathlib import Path
from typing import List, Optional

from yapim import InputLoader
from yapim.utils.package_manager import PackageManager


class PackageGenerator(PackageManager):
    def __init__(self,
                 tasks_directory: Path,
                 dependencies_directories: Optional[List[Path]],
                 input_loader_path: Optional[Path] = None):
        self._tasks_directory = os.path.basename(tasks_directory)
        if input_loader_path is not None and PackageGenerator._loader_type_is_valid(str(input_loader_path)):
            self._loader = os.path.basename(input_loader_path)
        else:
            self._loader = None
        if dependencies_directories is not None:
            self._dependencies_directories = dependencies_directories
        else:
            self._dependencies_directories = []

    def create(self, write_directory: str):
        write_directory = Path(os.getcwd()).resolve().joinpath(write_directory)
        if not write_directory.exists():
            os.makedirs(write_directory)
        output_data = {
            "loader": self._loader,
            "dependencies": [
                os.path.basename(dependency_directory)
                for dependency_directory in self._dependencies_directories
                if self._dependencies_directories is not None
            ],
            "tasks": os.path.basename(self._tasks_directory)
        }
        # Create pipeline base directory
        if not write_directory.exists():
            os.makedirs(write_directory)
        # Copy pipeline contents to own directory
        shutil.copytree(self._tasks_directory, output_data["tasks"], symlinks=True)
        # Copy loader
        if self._loader is not None:
            shutil.copy(self._loader, write_directory)
        # Copy all dependency directories
        for pre, post in zip(self._dependencies_directories, output_data["dependencies"]):
            shutil.copytree(pre, write_directory.joinpath(post), symlinks=True)
        # Save metadata file
        pipeline_file = write_directory.joinpath(PackageGenerator.pipeline_file)
        if not pipeline_file.exists() or input("Overwrite existing pipeline file? [Y/n]: ").upper() == "Y":
            with open(pipeline_file, "wb") as file_ptr:
                pickle.dump(output_data, file_ptr)

    @staticmethod
    def _loader_type_is_valid(loader_name: str) -> bool:
        return issubclass(PackageGenerator._get_loader(loader_name), InputLoader)
