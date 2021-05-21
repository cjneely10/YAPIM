"""
Manages the config file, as well as arguments that are set for each part of the pipeline

"""

import os
from pathlib import Path
from typing import List, Tuple, Dict, Optional

import yaml
from plumbum import local, CommandNotFound


class ImproperInputSection(ValueError):
    """ Wraps error of improperly formatted input section for deriving pipeline input

    """


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


class MissingDependenciesError(ValueError):
    """ Wraps ValueError, raise if dependency section is missing from config file

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
    SKIP = "skip"

    def __init__(self, config_path: Path):
        """ Create ConfigManager object

        :param config_path: Path to .yaml config file
        """
        with open(str(Path(config_path).resolve()), "r") as fp:
            self.config = yaml.load(fp, Loader=yaml.FullLoader)
            # Confirm all paths in file are valid
            ConfigManager._validate(self.config)

    def get(self, task_data: Tuple[str, str]) -> dict:
        """ Get (scope, name) data from config file

        :return:
        :rtype:
        """
        if task_data[0] == ConfigManager.ROOT:
            return self.config[task_data[1]]
        else:
            if ConfigManager.DEPENDENCIES not in self.config[task_data[0]].keys():
                raise MissingDependenciesError("Config file is missing valid dependencies section for step")
            return self.config[task_data[0]][ConfigManager.DEPENDENCIES][task_data[1]]

    def find(self, task_data: Tuple[str, str], key: str) -> Optional:
        config_section = self.get(task_data)
        if key in config_section.keys():
            return config_section[key]
        inner = self.parent_info(task_data)
        if key in inner.keys():
            return inner[key]

    def parent_info(self, task_data: Tuple[str, str]) -> dict:
        if task_data[0] == ConfigManager.ROOT:
            return self.config[task_data[1]]
        else:
            return self.config[task_data[0]]

    @staticmethod
    def _validate(data_dict):
        """ Confirm that data and dependency paths provided in file are all valid.

        :raises: MissingDataError

        """
        if not isinstance(data_dict, dict):
            raise MissingDataError("Dependency section is improperly configured!")
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
            # Provided as dict with program path and FLAGS
            if isinstance(prog_data, dict):
                try:
                    if "program" not in prog_data:
                        # pylint: disable=raise-missing-from
                        raise InvalidPathError(
                            "Dependency %s is missing a program path in your config file!" % prog_name
                        )
                    bool(local[prog_data["program"]])
                except CommandNotFound:
                    # pylint: disable=raise-missing-from
                    raise InvalidPathError(
                        "Dependency %s (provided: %s) is not present in your system's path!" % (
                            prog_name, prog_data["program"]))
            else:
                raise MissingDataError("Dependency section is improperly configured!")

    def get_slurm_flagged_arguments(self) -> List[Tuple[str, str]]:
        """ Get SLURM arguments from file

        :return: SLURM arguments parsed to input list
        """
        return sorted([
            (key, str(val)) for key, val in self.config["SLURM"].items()
            if key not in {"USE_CLUSTER", "--nodes", "--ntasks", "--mem", "user-id"}
        ], key=lambda v: v[0])

    def get_slurm_userid(self):
        """ Get user id from slurm section.

        :raises: MissingDataError if not present

        :return: user-id provided in config file
        """
        if "user-id" not in self.config["SLURM"].keys():
            raise MissingDataError("SLURM section missing required user data")
        return self.config["SLURM"]["user-id"]