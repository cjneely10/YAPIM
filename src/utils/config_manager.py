"""
Manages the config file, as well as arguments that are set for each part of the pipeline

"""

import os
from pathlib import Path
from typing import List, Tuple, Dict

import yaml
from plumbum import local, CommandNotFound


class InvalidPathError(FileExistsError):
    """ Wraps FileExistsError, raise if user provided a path that does not exist

    """
    pass


class MissingDataError(FileExistsError):
    """ Wraps FileExistsError, raise if user did not fill out a required DATA
    section in a configuration file

    """
    pass


class InvalidProtocolError(ValueError):
    """ Wraps ValueError, raise if user requests to use a protocol that isn't implemented

    """
    pass


class ConfigManager:
    """ ConfigManager handles parsing user-passed config file

    """
    EXPECTED_RESULTS_DIR = "results"
    ROOT = "root"
    SLURM = "SLURM"
    INPUT = "INPUT"
    THREADS = "threads"
    WORKERS = "workers"
    MEMORY = "memory"
    TIME = "time"
    PROTOCOL = "protocol"
    USE_CLUSTER = "USE_CLUSTER"
    DEPENDENCIES = "dependencies"
    PROGRAM = "program"
    FLAGS = "FLAGS"
    DATA = "data"
    BASE = "base"

    def __init__(self, config_path: Path):
        """ Create ConfigManager object

        :param config_path: Path to .yaml config file
        """
        self._config = yaml.load(open(str(Path(config_path).resolve()), "r"), Loader=yaml.FullLoader)
        # Confirm all paths in file are valid
        ConfigManager._validate(self._config)

    def get(self, task_data: Tuple[str, str]) -> dict:
        """ Get (scope, name) data from config file

        :return:
        :rtype:
        """
        if task_data[0] == ConfigManager.ROOT:
            return self._config[task_data[1]]
        else:
            return self._config[task_data[0]][ConfigManager.DEPENDENCIES][task_data[1]]

    def parent_info(self, task_data: Tuple[str, str]) -> dict:
        if task_data[0] == ConfigManager.ROOT:
            return self._config[task_data[1]]
        else:
            return self._config[task_data[0]]

    @staticmethod
    def _validate(data_dict):
        """ Confirm that data and dependency paths provided in file are all valid.

        :raises: MissingDataError

        """
        for task_name, task_dict in data_dict.items():
            if "skip" in task_dict.keys() and task_dict["skip"] is True:
                continue
            if "data" in task_dict.keys():
                for _val in task_dict["data"].split(","):
                    if ":" in _val:
                        _val = _val.split(":")[1]
                    if not os.path.exists(str(Path(_val).resolve())):
                        raise MissingDataError("Data for task %s (provided: %s) does not exist!" % (
                            task_name, _val
                        ))
            if "dependencies" in task_dict.keys():
                ConfigManager._validate(task_dict["dependencies"])
                ConfigManager._check_dependencies(task_dict)

    @staticmethod
    def _check_dependencies(task_dict: Dict):
        """ Check dependencies section of config file section

        :param task_dict: dictionary section for task
        :raises: MissingDataError for poorly formed or missing data
        """
        for prog_name, prog_data in task_dict["dependencies"].items():
            # Simple - is only a path with no ability to pass flags
            if isinstance(prog_data, str):
                try:
                    local[prog_data]
                except CommandNotFound:
                    # pylint: disable=raise-missing-from
                    raise MissingDataError(
                        "Dependency %s (provided: %s) is not present in your system's path!" % (
                            prog_name, prog_data))
            # Provided as dict with program path and FLAGS
            elif isinstance(prog_data, dict):
                try:
                    if "program" not in prog_data:
                        # pylint: disable=raise-missing-from
                        raise InvalidPathError(
                            "Dependency %s is improperly configured in your config file!" % prog_name
                        )
                    bool(prog_data["program"])
                except CommandNotFound:
                    # pylint: disable=raise-missing-from
                    raise MissingDataError(
                        "Dependency %s (provided: %s) is not present in your system's path!" % (
                            prog_name, prog_data["program"]))

    def get_slurm_flagged_arguments(self) -> List[Tuple[str, str]]:
        """ Get SLURM arguments from file

        :return: SLURM arguments parsed to input list
        """
        return [
            (key, str(val)) for key, val in self._config["SLURM"].items()
            if key not in {"USE_CLUSTER", "--nodes", "--ntasks", "--mem", "user-id"}
        ]

    def get_slurm_userid(self):
        """ Get user id from slurm section.

        :raises: MissingDataError if not present

        :return: user-id provided in config file
        """
        if "user-id" not in self._config["SLURM"].keys():
            raise MissingDataError("SLURM section missing required user data")
        return self._config["SLURM"]["user-id"]
