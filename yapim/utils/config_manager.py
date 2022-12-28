"""Manages the config file, as well as arguments that are set for each part of the pipeline"""

import os
from pathlib import Path
from typing import List, Tuple, Dict, Optional

import yaml
from plumbum import local, CommandNotFound


class InvalidResourcesError(AttributeError):
    """When a task requests more resources than are globally available"""


class MissingRequiredHeader(AttributeError):
    """When a file is missing either INPUT, GLOBAL, or SLURM"""


class MissingTimingData(AttributeError):
    """When a config file is missing required sections"""


class MissingProgramSection(AttributeError):
    """When a program is requested to be used, but section is not present in Task's
    config file section"""


class ImproperInputSection(ValueError):
    """Wraps error of improperly formatted input section for deriving pipeline input"""


class InvalidPathError(FileExistsError):
    """Wraps FileExistsError, raise if user provided a path that does not exist"""
    pass


class MissingDataError(FileExistsError):
    """Wraps FileExistsError, raise if user did not fill out a required DATA
    section in a configuration file"""
    pass


class InvalidProtocolError(ValueError):
    """Wraps ValueError, raise if user requests to use a protocol that isn't implemented"""
    pass


class MissingDependenciesError(ValueError):
    """Wraps ValueError, raise if dependency section is missing from config file"""
    pass


class ConfigManager:
    """ConfigManager handles parsing user-passed config file"""
    ROOT = "root"
    SLURM = "SLURM"
    SBATCH = "SBATCH"
    SLURM_HEADER = "SLURM_HEADER"
    INPUT = "INPUT"
    NODES = "nodes"
    TASKS = "tasks"
    THREADS = "threads"
    WORKERS = "workers"
    MEMORY = "memory"
    TIME = "time"
    USE_CLUSTER = "USE_CLUSTER"
    DEPENDENCIES = "dependencies"
    PROGRAM = "program"
    FLAGS = "FLAGS"
    DATA = "data"
    SKIP = "skip"
    MAX_THREADS = "MaxThreads"
    MAX_MEMORY = "MaxMemory"
    GLOBAL = "GLOBAL"

    def __init__(self, config_path: Path, storage_directory: Optional[Path] = None):
        with open(str(Path(config_path).resolve()), "r") as file_ptr:
            self.config = yaml.load(file_ptr, Loader=yaml.FullLoader)
            # Confirm all paths in file are valid
            self._validate_global()
        self.storage_directory = storage_directory

    def get(self, task_data: Tuple[str, str]) -> dict:
        """Get (scope, name) data from config file"""
        if task_data[0] == ConfigManager.ROOT:
            return self.config[task_data[1]]
        if ConfigManager.DEPENDENCIES not in self.config[task_data[0]].keys():
            raise MissingDependenciesError("Config file is missing valid dependencies section for step")
        return self.config[task_data[0]][ConfigManager.DEPENDENCIES][task_data[1]]

    def find(self, task_data: Tuple[str, str], key: str) -> Optional:
        """Find a Task's data `key`. If not found in Task's own config section, check parent sections.
        Return None if nothing is found."""
        config_section = self.get(task_data)
        if key in config_section.keys():
            return config_section[key]
        inner = self.parent_info(task_data)
        if key in inner.keys():
            return inner[key]
        return None

    def parent_info(self, task_data: Tuple[str, str]) -> dict:
        """Get this Task's parent info, which may be the top-level ConfigManager.ROOT location"""
        if task_data[0] == ConfigManager.ROOT:
            return self.config[task_data[1]]
        return self.config[task_data[0]]

    # pylint: disable=raise-missing-from
    def _validate_global(self):
        """Confirm global settings are present and valid"""
        data_dict = self.config
        for required_arg in (ConfigManager.GLOBAL, ConfigManager.INPUT, ConfigManager.SLURM):
            if required_arg not in data_dict.keys():
                raise MissingRequiredHeader(f"Config section {required_arg} is missing!")
        for required_arg in (ConfigManager.MAX_MEMORY, ConfigManager.MAX_THREADS):
            if required_arg not in data_dict[ConfigManager.GLOBAL]:
                raise MissingRequiredHeader(f"Global argument {required_arg} is missing!")
            try:
                int(data_dict[ConfigManager.GLOBAL][required_arg])
            except ValueError:
                raise MissingRequiredHeader(f"Global argument {required_arg} is not an integer!")
        max_memory = int(data_dict[ConfigManager.GLOBAL][ConfigManager.MAX_MEMORY])
        max_threads = int(data_dict[ConfigManager.GLOBAL][ConfigManager.MAX_THREADS])
        ConfigManager._validate(self.config, False, max_memory, max_threads)

    # pylint: disable=too-many-branches
    @staticmethod
    def _validate(data_dict, is_dependency: bool, max_memory: int, max_threads: int):
        """ Confirm that data and dependency paths provided in file are all valid."""
        if not isinstance(data_dict, dict):
            raise MissingDataError("Dependency section is improperly configured!")
        for task_name, task_dict in data_dict.items():
            if task_dict is None:
                raise MissingDataError(
                    f"Config region for {'dependency ' if is_dependency else ''}{task_name} is empty or malformed")
            if not is_dependency and task_name not in (ConfigManager.INPUT, ConfigManager.SLURM, ConfigManager.GLOBAL):
                for required_arg in (ConfigManager.MEMORY, ConfigManager.THREADS, ConfigManager.TIME):
                    # Verify that the required args are defined
                    if required_arg not in task_dict.keys():
                        raise MissingTimingData(f"Config section for {task_name} is missing required definition "
                                                f"'{required_arg}'")
                task_requirements = {ConfigManager.THREADS: 0, ConfigManager.MEMORY: 0}
                # pylint: disable=consider-iterating-dictionary
                for required_arg in task_requirements.keys():
                    # Parse argument for numeric, positive value
                    try:
                        req_res_value = int(task_dict[required_arg])
                        if req_res_value <= 0:
                            raise InvalidResourcesError(f"'{required_arg}' should be a positive value")
                        task_requirements[required_arg] = req_res_value
                    except ValueError:
                        raise InvalidResourcesError(f"Requested resource '{required_arg}' must be numeric")
                threads = task_requirements[ConfigManager.THREADS]
                if threads > max_threads:
                    raise InvalidResourcesError(f"Max threads is set to {max_threads} "
                                                f"but {task_name} requests {threads}")
                memory = task_requirements[ConfigManager.MEMORY]
                if memory > max_memory:
                    raise InvalidResourcesError(f"Max memory is set to {max_memory} "
                                                f"but {task_name} requests {memory}")
            if ConfigManager.SKIP in task_dict.keys() and task_dict[ConfigManager.SKIP] is True:
                continue
            if ConfigManager.DATA in task_dict.keys():
                for _val in ConfigManager._parse_flags(task_dict[ConfigManager.DATA]):
                    if ":" in _val:
                        _val = _val.split(":")[1]
                    if not os.path.exists(str(Path(_val).resolve())):
                        raise MissingDataError("Data for task %s (provided: %s) does not exist!" % (
                            task_name, _val
                        ))
            if ConfigManager.PROGRAM in task_dict.keys():
                try:
                    local.which(task_dict[ConfigManager.PROGRAM])
                except CommandNotFound:
                    # pylint: disable=raise-missing-from
                    raise InvalidPathError(
                        "Task %s (provided program path: %s) is not present in your system's path!" % (
                            task_name, task_dict[ConfigManager.PROGRAM]))
            if "dependencies" in task_dict.keys():
                ConfigManager._validate(task_dict["dependencies"], True, max_memory, max_threads)
                ConfigManager._check_dependencies(task_dict)

    # pylint: disable=fixme
    # TODO: Handle empty input dictionaries for config-level dependencies
    @staticmethod
    def _check_dependencies(task_dict: Dict):
        """ Check dependencies section of config file section

        :param task_dict: dictionary section for task
        :raises: MissingDataError for poorly formed or missing data
        """
        for prog_name, prog_data in task_dict["dependencies"].items():
            # Provided as dict with program path and FLAGS
            if isinstance(prog_data, dict):
                provided_program = ""
                try:
                    if ConfigManager.PROGRAM in prog_data.keys():
                        provided_program = prog_data[ConfigManager.PROGRAM]
                        if not os.path.exists(provided_program):
                            local.which(provided_program)
                except CommandNotFound as not_found_err:
                    raise InvalidPathError(
                        "Dependency %s (program path provided: %s) is not present in your system's path!" % (
                            prog_name, provided_program)) from not_found_err
            else:
                raise MissingDataError("Dependency section is improperly configured!")

    def get_sbatch_flagged_arguments(self) -> List[Tuple[str, str]]:
        """ Get SLURM arguments from file

        :return: SLURM arguments parsed to input list
        """
        ignore_slurm_fields = {"USE_CLUSTER", "--nodes", "--ntasks", "--mem", "user-id"}
        slurm_section_data = {key: str(val)
                              for key, val in self.config[ConfigManager.SLURM].items()
                              if key not in ignore_slurm_fields}
        return sorted(((key, value) for key, value in slurm_section_data.items()), key=lambda v: v[0])

    def get_slurm_userid(self):
        """ Get user id from slurm section.

        :raises: MissingDataError if not present
        :return: user-id provided in config file
        """
        if "user-id" not in self.config["SLURM"].keys():
            raise MissingDataError("SLURM section missing required user data")
        return self.config["SLURM"]["user-id"]

    @staticmethod
    def flags_to_list(cfg_manager: "ConfigManager", task, config_param: str):
        """ Get additional flags from given section, parsed to list

        Example: self.local["ls"][(*self.added_flags("ls"))]

        :return: List of arguments to pass to calling program
        """
        flags = cfg_manager.find(task.full_name, config_param)
        return ConfigManager._parse_flags(flags)

    @staticmethod
    def _parse_flags(flags):
        """Parse config section into a list"""
        if flags is not None:
            out = flags.split(" ")
            while "" in out:
                out.remove("")
            return out
        return []
