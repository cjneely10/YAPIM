"""Pipeline package generation utilities"""
import errno
import os
import pickle
import shutil
from pathlib import Path
from typing import List, Optional

from yapim.utils.package_management.config_manager_generator import ConfigManagerGenerator
from yapim.utils.package_management.package_manager import PackageManager


# pylint: disable=too-few-public-methods
class PackageGenerator(PackageManager):
    """Create user-deliverable package from pipeline contents"""
    def __init__(self,
                 tasks_directory: Path,
                 dependencies_directories: Optional[List[Path]],
                 input_loader_path: Optional[Path] = None):
        """
        Create generator for a YAPIM pipeline

        :param tasks_directory: Directory of pipeline tasks
        :param dependencies_directories: List of dependency directories that were used
        :param input_loader_path: Path to input loader. If not provided, will default to `ExtensionLoader`.
        """
        self._tasks_directory = tasks_directory
        self._loader = input_loader_path
        if dependencies_directories is not None:
            self._dependencies_directories = dependencies_directories
        else:
            self._dependencies_directories = []

    def create(self, write_directory: Path):
        """
        Create packaged pipeline

        :param write_directory: Output directory
        :raises OSError: If unable to copy contents
        """
        if not write_directory.exists():
            os.makedirs(write_directory)
        output_data = {
            "loader": self._loader is not None,
            "dependencies": [
                os.path.basename(dependency_directory)
                for dependency_directory in self._dependencies_directories
                if self._dependencies_directories is not None
            ],
            "tasks": os.path.basename(self._tasks_directory)
        }
        # Copy pipeline contents to own directory
        PackageGenerator._try_copy(self._tasks_directory, write_directory.joinpath(output_data["tasks"]),
                                   symlinks=True, dirs_exist_ok=True)
        # Copy loader
        if self._loader is not None:
            shutil.copy(self._loader, write_directory)
        # Copy all dependency directories
        for pre, post in zip(self._dependencies_directories, output_data["dependencies"]):
            PackageGenerator._try_copy(pre, write_directory.joinpath(post), symlinks=True, dirs_exist_ok=True)
        # Save metadata file
        pipeline_file = write_directory.joinpath(super().pipeline_file)
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

    @staticmethod
    def _try_copy(pre: Path, post: Path, *args, **kwargs):
        """
        Attempt to copy directory contents.

        Per "https://bugs.python.org/issue38633", `shutil` may raise error on WSL.
        Apply patch described and reattempt. If fails on this error, will not be caught within function.

        :param pre: Source path
        :param post: Destination path
        :raises OSError: If fails to copy despite presence of WSL patch
        """
        try:
            shutil.copytree(pre, post, *args, **kwargs)
        except shutil.Error:
            # pylint: disable=protected-access
            orig_copyxattr = shutil._copyxattr

            def patched_copyxattr(src, dst, *, follow_symlinks=True):
                try:
                    orig_copyxattr(src, dst, follow_symlinks=follow_symlinks)
                except OSError as ex:
                    if ex.errno != errno.EACCES:
                        raise

            # pylint: disable=protected-access
            shutil._copyxattr = patched_copyxattr
            shutil.copytree(pre, post, *args, **kwargs)
