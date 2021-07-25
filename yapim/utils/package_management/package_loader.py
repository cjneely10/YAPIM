import pickle
from pathlib import Path
from typing import Dict, Type, Tuple, List, Optional

from yapim import Task
from yapim.tasks.utils.loader import get_modules
from yapim.utils.extension_loader import ExtensionLoader
from yapim.utils.input_loader import InputLoader
from yapim.utils.package_management.package_manager import PackageManager


class PackageLoader(PackageManager):
    def __init__(self, pipeline_package_directory: Path):
        self._pipeline_directory = pipeline_package_directory

    def validate_pipeline_pkl(self) -> dict:
        pipeline_pkl_path = self._pipeline_directory.joinpath(PackageLoader.pipeline_file)
        if not pipeline_pkl_path.exists():
            print("Unable to find pipeline .pkl file")
            exit(1)
        pipeline_data: dict
        with open(pipeline_pkl_path, "rb") as provided_pipeline_file:
            pipeline_data = pickle.load(provided_pipeline_file)
        for key in ("tasks", "dependencies"):
            if key not in pipeline_data.keys() or \
                    (isinstance(pipeline_data[key], Path)
                     and not self._pipeline_directory.joinpath(pipeline_data[key]).exists()):
                print(f"Unable to load {key} {pipeline_data[key]}")
                print(f"Re-run yaml config to update pipeline")
                exit(1)
        # Load task/dependencies at directory level
        pipeline_data["tasks"] = self._pipeline_directory.joinpath(pipeline_data["tasks"])
        pipeline_data["dependencies"] = [
            self._pipeline_directory.joinpath(dependency_directory)
            for dependency_directory in pipeline_data["dependencies"]
        ]
        # Load and validate input loader
        loader_path = pipeline_data.get("loader")
        if loader_path is None or loader_path is False:
            pipeline_data["loader"] = ExtensionLoader
        else:
            loader = PackageLoader._get_loader(self._pipeline_directory)
            if not issubclass(loader, InputLoader):
                print(f"Unable to validate loader")
                exit(1)
            pipeline_data["loader"] = loader
        return pipeline_data

    def load_from_package(self) -> Tuple[List[Type[Task]], Dict[str, Type[Task]]]:
        pipeline_data = self.validate_pipeline_pkl()
        return PackageLoader.load_from_directories(pipeline_data["tasks"], pipeline_data["dependencies"])

    @staticmethod
    def load_from_directories(
            tasks_directory: Path,
            dependencies_directories: Optional[List[Path]]) -> Tuple[List[Type[Task]], Dict[str, Type[Task]]]:
        task_blueprints: Dict[str, Type[Task]] = get_modules(tasks_directory)
        pipeline_tasks = list(task_blueprints.values())
        if dependencies_directories is not None and len(dependencies_directories) > 0:
            for directory in dependencies_directories:
                task_blueprints.update(get_modules(directory))
        return pipeline_tasks, task_blueprints
