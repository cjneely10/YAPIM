import os
import pickle
import shutil
from pathlib import Path
from typing import List, Optional

from yapim import InputLoader
from yapim.utils.package_management.config_manager_generator import ConfigManagerGenerator
from yapim.utils.package_management.package_manager import PackageManager


class PackageGenerator(PackageManager):
    def __init__(self,
                 tasks_directory: Path,
                 dependencies_directories: Optional[List[Path]],
                 input_loader_path: Optional[Path] = None):
        self._tasks_directory = tasks_directory
        # if input_loader_path is not None and PackageGenerator._loader_type_is_valid(str(input_loader_path)):
        self._loader = input_loader_path
        # else:
        #     self._loader = None
        if dependencies_directories is not None:
            self._dependencies_directories = dependencies_directories
        else:
            self._dependencies_directories = []

    def create(self, write_directory: Path):
        if not write_directory.exists():
            os.makedirs(write_directory)
        output_data = {
            "loader": (True if self._loader is not None else False),
            "dependencies": [
                os.path.basename(dependency_directory)
                for dependency_directory in self._dependencies_directories
                if self._dependencies_directories is not None
            ],
            "tasks": os.path.basename(self._tasks_directory)
        }
        # Copy pipeline contents to own directory
        shutil.copytree(self._tasks_directory, write_directory.joinpath(output_data["tasks"]),
                        symlinks=True, dirs_exist_ok=True)
        # Copy loader
        if self._loader is not None:
            shutil.copy(self._loader, write_directory)
        # Copy all dependency directories
        for pre, post in zip(self._dependencies_directories, output_data["dependencies"]):
            shutil.copytree(pre, write_directory.joinpath(post), symlinks=True, dirs_exist_ok=True)
        # Save metadata file
        pipeline_file = write_directory.joinpath(PackageGenerator.pipeline_file)
        # Create config stuff
        self._create_config(write_directory)
        # Make a python package
        open(write_directory.joinpath("__init__.py"), "a").close()
        # Store data
        with open(pipeline_file, "wb") as file_ptr:
            pickle.dump(output_data, file_ptr)

    def _create_config(self, write_directory: Path):
        config_file_path = write_directory.joinpath(os.path.basename(self._tasks_directory) + "-config.yaml")
        if not config_file_path.exists() or input("Overwrite existing configuration file? [Y/n]: ").upper() == "Y":
            ConfigManagerGenerator(self._tasks_directory, self._dependencies_directories).write(config_file_path)
