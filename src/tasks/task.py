import logging
import os
import time
import traceback
from abc import ABC
from pathlib import Path
from typing import Tuple, List, Optional, Callable

from plumbum import local, colors
from plumbum.machines import LocalMachine, LocalCommand

from src.tasks.base_task import BaseTask
from src.tasks.utils.result import Result
from src.tasks.utils.slurm_caller import SLURMCaller
from src.utils.config_manager import ConfigManager, MissingDataError


def set_complete(func: Callable):
    """ Decorator function that checks whether a given task is completed. Check on Task object creation, but post-
    child-class self.output update

    :param func: Task class initializer method
    :return: Decorated function, class object modified to store updated self.is_complete status
    """
    def _check_if_complete(self, *args, **kwargs):
        func(self, *args, **kwargs)
        self.is_complete = self.set_is_complete()
    return _check_if_complete


class Task(BaseTask, ABC):
    def __init__(self, record_id: str, task_scope: str, result_map, wdir: str):
        self.record_id: str = record_id
        self._task_scope = task_scope
        self.input: dict = result_map[self.record_id]
        self.output = {}
        self.wdir: Path = Path(wdir).resolve()
        self.config: dict = result_map.config_manager.get(self.full_name)
        self.results_map = result_map
        parent_data = result_map.config_manager.parent_info(self.full_name)
        self.is_skip = "skip" in self.config.keys() and self.config["skip"] is True or \
                       "skip" in parent_data.keys() and parent_data["skip"] is True
        self.is_complete = False

    @property
    def task_scope(self) -> str:
        return self._task_scope

    @property
    def threads(self) -> str:
        """ Number of threads when running task (as set in config file)

        :return: Str of number of tasks
        """
        return self.config[ConfigManager.THREADS]

    @property
    def added_flags(self) -> List[str]:
        """ Get additional flags that user provided in config file

        Example: self.local["ls"][(*self.added_flags("ls"))]

        :return: List of arguments to pass to calling program
        """
        if isinstance(self.config[self.task_name], dict):
            if ConfigManager.FLAGS in self.config[self.task_name].keys():
                out = self.config[self.task_name][ConfigManager.FLAGS].split(" ")
                while "" in out:
                    out.remove("")
                return out
        return []

    @property
    def is_slurm(self) -> bool:
        """
        Run was launched on SLURM
        """
        return bool(self.slurm_config[ConfigManager.USE_CLUSTER])

    def _create_slurm_command(self, cmd: LocalCommand, time_override: Optional[str] = None,
                              threads_override: str = None, memory_override: str = None) -> SLURMCaller:
        """ Create a SLURM-managed process

        :param cmd: plumbum LocalCommand object to run
        :param time_override: Time override in "HH:MM:SS" format, if needed
        :param threads_override: Provide number of threads to parallelize over, default to use config-level threads-pw.
            Note that this will only affect SLURM script generation - this will not override thread values passed in by
            the cmd parameter
        :param memory_override: Provide memory override for command in "2GB" format, etc.
        :return: SLURM-wrapped command to run script via plumbum interface
        """
        # Confirm valid SLURM section
        if ConfigManager.MEMORY not in self.parent_data.keys():
            raise MissingDataError("SLURM section not properly formatted within %s" % self.full_name)
        if ConfigManager.TIME not in self.parent_data.keys():
            raise MissingDataError("SLURM section not properly formatted within %s" % self.full_name)
        # Generate command to launch SLURM job
        return SLURMCaller(
            self.res.get_slurm_userid(),
            self.wdir,
            str(self._threads_pw) if threads_override is None else threads_override,
            cmd,
            self.memory if memory_override is None else memory_override,
            self.time if time_override is None else time_override,
            self.local,
            self.cfg.get_slurm_flagged_arguments(),
        )

    @property
    def data(self) -> List[str]:
        """ List of data files passed to this task's config section

        :return: List of paths of data in task's config section
        """
        return self.config[ConfigManager.DATA].split(" ")

    def run_task(self) -> Result:
        """ Type of run. For Task objects, this simply calls run(). For other tasks, there
        may be more processing required prior to returning the result.

        This method will be used to return the result of the child Task class implemented run method.

        :return:
        """
        if self.is_skip:
            return Result(self.record_id, self.task_name, {})

        if not self.is_complete:
            _str = "In progress:  {}".format(self.record_id)
            logging.info(_str)
            print(colors.blue & colors.bold | _str)
            start_time = time.time()
            try:
                self.run()
            # pylint: disable=broad-except
            except BaseException as err:
                logging.info(err)
                logging.info(traceback.print_exc())
                with open(os.path.join(self.wdir, "task.err"), "a") as w_out:
                    w_out.write(str(err) + "\n")
                    w_out.write(traceback.format_exc() + "\n")
                print(colors.warn | str(err))
            end_time = time.time()
            _str = "Is complete:  {} ({:.3f}{})".format(self.record_id, *Task._parse_time(end_time - start_time))
            logging.info(_str)
            print(colors.blue & colors.bold | _str)
        else:
            _str = "Is complete: {}".format(self.record_id)
            logging.info(_str)
            print(colors.blue & colors.bold | _str)

        for key, output in self.output.items():
            if isinstance(output, Path) and not output.exists():
                raise super().TaskCompletionError(key, output)
        return Result(self.record_id, self.task_name, self.output)

    @property
    def local(self) -> LocalMachine:
        return local

    @property
    def program(self) -> LocalCommand:
        return self.local[self.config[ConfigManager.PROGRAM]]

    def __str__(self):
        return f"<Task name: {self.task_name}, scope: {self.task_scope}, input_id: {self.record_id}, requirements: {self.requires}, dependencies: {self.depends}>"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def _parse_time(_time: float) -> Tuple[float, str]:
        """ Parse time to complete a task into
        day, hour, minute, or second representation based on scale

        :param _time: Time to complete a given task
        :return: time and string representing unit
        """
        if _time > 3600 * 24:
            return _time / (3600 * 24), "d"
        if _time > 3600:
            return _time / 3600, "h"
        if _time > 60:
            return _time / 60, "m"
        return _time, "s"

    def set_is_complete(self) -> bool:
        """ Check all required output data to see if any part of task need to be completed

        :return: Boolean representing if task has all required output
        """
        is_complete = None
        for _path in self.output.values():
            if isinstance(_path, Path):
                if not _path.exists():
                    # Only call function if missing path
                    # Then move on
                    is_complete = False
                    break
                is_complete = True
        if is_complete is None:
            return False
        return is_complete
