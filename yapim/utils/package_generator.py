import os
import pickle
import shutil
from importlib import import_module
from inspect import isclass
from pathlib import Path
from typing import List, Optional, Type

from yapim import InputLoader


class PackageGenerator:
    pipeline_file = ".pipeline"

    def __init__(self,
                 tasks_directory: Path,
                 dependencies_directories: Optional[List[Path]],
                 input_loader_path: Optional[Path] = None):
        self.tasks_directory = tasks_directory
        if input_loader_path is not None and PackageGenerator._loader_type_is_valid(str(input_loader_path)):
            self.loader = input_loader_path
        else:
            self.loader = None
        if dependencies_directories is not None:
            self.dependencies_directories = dependencies_directories
        else:
            self.dependencies_directories = []

    def create(self, write_directory: Path):
        output_data = {
            "loader": self.loader,
            "dependencies_directories": [
                write_directory.joinpath(os.path.basename(dependency_directory))
                for dependency_directory in self.dependencies_directories
                if self.dependencies_directories is not None
            ],
            "pipeline": write_directory.joinpath(os.path.basename(self.tasks_directory))
        }
        # Create pipeline base directory
        if not write_directory.exists():
            os.makedirs(write_directory)
        # Copy pipeline contents to own directory
        shutil.copytree(self.tasks_directory, output_data["pipeline"])
        # Copy all dependency directories
        for pre, post in zip(self.dependencies_directories, output_data["dependencies_directories"]):
            shutil.copytree(pre, post)
        # Save metadata file
        pipeline_file = write_directory.joinpath(PackageGenerator.pipeline_file)
        if not pipeline_file.exists() or input("Overwrite existing pipeline file? [Y/n]: ").upper() == "Y":
            with open(pipeline_file, "wb") as file_ptr:
                pickle.dump(output_data, file_ptr)

    @staticmethod
    def _loader_type_is_valid(loader_name: str) -> bool:
        return issubclass(PackageGenerator._get_loader(loader_name), InputLoader)

    @staticmethod
    def _get_loader(loader_name: str) -> Type[InputLoader]:
        current_dir = os.getcwd()
        os.chdir(os.path.dirname(loader_name))
        module = import_module(os.path.basename(os.path.splitext(loader_name)[0]))
        for attribute_name in dir(module):
            attribute = getattr(module, attribute_name)
            if isclass(attribute) and issubclass(attribute, InputLoader) and loader_name in attribute.__name__:
                os.chdir(current_dir)
                return attribute
        os.chdir(current_dir)
        print("Unable to import loader module")
        exit(1)
