import logging
import os
import time
import traceback
from abc import ABC
from pathlib import Path
from typing import Tuple, List, Optional, Union

from plumbum import local, colors
from plumbum.machines import LocalMachine, LocalCommand

from HPCBioPipe.tasks.utils.InputDict import InputDict
from HPCBioPipe.tasks.utils.base_task import BaseTask
from HPCBioPipe.tasks.utils.result import Result
from HPCBioPipe.tasks.utils.slurm_caller import SLURMCaller
from HPCBioPipe.utils.config_manager import ConfigManager, MissingDataError


class TaskSetupError(AttributeError):
    """
    Wrap AttributeError around improperly-formatted Task fields
    """


class Task(BaseTask, ABC):
    def __init__(self, record_id: str, task_scope: str, result_map,
                 added_data: dict, wdir: str, display_messages: bool):
        self.record_id: str = record_id
        self._task_scope = task_scope
        added_data.update(result_map.get(self.record_id, {}))
        self.input: InputDict = InputDict(added_data)
        self.output = {}
        self.wdir: Path = Path(wdir).resolve()
        self.results_map = result_map
        is_skip = self.results_map.config_manager.find(self.full_name, ConfigManager.SKIP)
        if is_skip is not None and str(is_skip) == "true":
            self.is_skip = True
        else:
            self.is_skip = False
        self.is_complete = False
        self.display_messages = display_messages

    def task_scope(self) -> str:
        return self._task_scope

    @property
    def threads(self) -> str:
        """ Number of threads when running task (as set in config file)

        :return: Str of number of tasks
        """
        return self.results_map.config_manager.find(self.full_name, ConfigManager.THREADS)

    @property
    def config(self) -> dict:
        return self.results_map.get(self.full_name)

    @property
    def added_flags(self) -> List[str]:
        """ Get additional flags that user provided in config file

        Example: self.local["ls"][(*self.added_flags("ls"))]

        :return: List of arguments to pass to calling program
        """
        flags = self.results_map.config_manager.find(self.full_name, ConfigManager.FLAGS)
        if flags is not None:
            out = flags.split(" ")
            while "" in out:
                out.remove("")
            return out
        return []

    @property
    def is_slurm(self) -> bool:
        """
        Run was launched on SLURM
        """
        return bool(self.results_map.config_manager.config[ConfigManager.SLURM][ConfigManager.USE_CLUSTER])

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
        parent_info: dict = self.results_map.parent_info(self.full_name)
        if ConfigManager.MEMORY not in self.config.keys() or ConfigManager.MEMORY not in parent_info.keys():
            raise MissingDataError("SLURM section not properly formatted within %s" % self.full_name)
        if ConfigManager.TIME not in self.config.keys() or ConfigManager.TIME not in parent_info.keys():
            raise MissingDataError("SLURM section not properly formatted within %s" % self.full_name)
        # Generate command to launch SLURM job
        return SLURMCaller(
            self.results_map.get_slurm_userid(),
            str(self.wdir),
            str(self.threads) if threads_override is None else threads_override,
            cmd,
            self.results_map.find(self.full_name, ConfigManager.MEMORY) if memory_override is None else memory_override,
            self.results_map.find(self.full_name, ConfigManager.TIME) if time_override is None else time_override,
            self.local,
            self.results_map.config_manager.get_slurm_flagged_arguments(),
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
            return Result(self.record_id, self.name, {})
        if self.display_messages:
            print("\nRunning:\n  %s" % (
                    (self.task_scope() + " " if self.task_scope() != ConfigManager.ROOT else "") + self.name
            ))

        if not self.is_complete:
            _str = "In progress:  {}".format(self.record_id)
            logging.info(_str)
            if self.display_messages:
                print(colors.blue & colors.bold | _str)
            start_time = time.time()
            self.try_run()
            end_time = time.time()
            _str = "Is complete:  {} ({:.3f}{})".format(self.record_id, *Task._parse_time(end_time - start_time))
            logging.info(_str)
            if self.display_messages:
                print(colors.blue & colors.bold | _str)
        else:
            _str = "Is complete: {}".format(self.record_id)
            logging.info(_str)
            if self.display_messages:
                print(colors.blue & colors.bold | _str)

        for key, output in self.output.items():
            if key != "final":
                if (isinstance(output, Path) and not output.exists()) or \
                        (isinstance(output, str) and not os.path.exists(output)):
                    raise super().TaskCompletionError(key, Path(output))
        return Result(self.record_id, self.name, self.output)

    @property
    def local(self) -> LocalMachine:
        return local

    @property
    def program(self) -> LocalCommand:
        return self.local[self.results_map.config_manager.find(self.full_name, ConfigManager.PROGRAM)]

    def __str__(self):  # pragma: no cover
        return f"<Task name: {self.__name__}, scope: {self.task_scope()}, input_id: {self.record_id}, " \
               f"requirements: {self.requires()}, dependencies: {self.depends()}>"

    def __repr__(self):  # pragma: no cover
        return self.__str__()

    def try_run(self):
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

    def set_is_complete(self):
        """ Check all required output data to see if any part of task need to be completed

        :return: Boolean representing if task has all required output
        """
        is_complete = None
        for _path in self.output.values():
            if isinstance(_path, Path) or isinstance(_path, str):
                if not os.path.exists(_path):
                    # Only call function if missing path
                    # Then move on
                    is_complete = False
                    break
                is_complete = True
        if is_complete is None:
            is_complete = False
        self.is_complete = is_complete

    def parallel(self, cmd: Union[LocalCommand, List[LocalCommand]], time_override: Optional[str] = None,
                 threads_override: str = None, memory_override: str = None):
        """ Launch a command that uses multiple threads
        This method will call a given command on a SLURM cluster automatically (if requested by the user)
        In a config file, WORKERS will correspond to the number of tasks to run in parallel. For slurm users, this
        is the number of jobs that will run simultaneously.

        A time-override may be specified to manually set the maximum time limit a command (job) may run on a cluster,
        which will override the time that is specified by the user in a config file

        The command string will be written to the EukMetaSanity pipeline output file and will be printed to screen

        Example:
        self.parallel(self.local["pwd"], "1:00")

        :param cmd: plumbum LocalCommand object to run, or list of commands to run
        :param time_override: Time override in "HH:MM:SS" format, if needed
        :param threads_override: Provide number of threads to parallelize over, default to use config-level threads-pw.
            Note that this will only affect SLURM script generation - this will not override thread values passed in by
            the cmd parameter
        :param memory_override: Provide memory override for command in "2GB" format, etc.
        :raises: MissingDataError if SLURM section improperly configured
        """
        # Write command to slurm script file and run
        if self.is_slurm:
            cmd = self._create_slurm_command(cmd, time_override, threads_override, memory_override)
        # Run command directly
        logging.info(str(cmd))
        if self.display_messages:
            print("  " + str(cmd))
        out = cmd()
        # Store log info in any was generated
        if out is not None:
            with open(os.path.join(self.wdir, "task.log"), "a") as w:
                w.write(str(out))
        if isinstance(cmd, SLURMCaller) and os.path.exists(cmd.slurm_log_file):
            with open(os.path.join(self.wdir, "task.log"), "a") as w:
                w.write("".join(open(cmd.slurm_log_file, "r").readlines()))

    def single(self, cmd: Union[LocalCommand, List[LocalCommand]],
               time_override: Optional[str] = None, memory_override: str = None):
        """ Launch a command that uses a single thread.

        The command string will be written to the EukMetaSanity pipeline output file and will be printed to screen

        Example:
        self.single(self.local["pwd"])

        :param cmd: plumbum LocalCommand object to run, or list of commands to run
        :param time_override: Time override in "HH:MM:SS" format, if needed
        :param memory_override: Provide memory override for command in "2GB" format, etc.
        """
        self.parallel(cmd, time_override, threads_override="1", memory_override=memory_override)

    def create_script(self, cmd: Union[str, LocalCommand, List[str]], file_name: str) -> LocalCommand:
        """ Write a command to file and return its value packaged as a LocalCommand.

        This is highly useful when incorporating programs that only launch in the directory in which it was called

        Example:

        script = self.create_script(self.local["ls"]["~"], "cd.sh")

        This will create a file within self.wdir named `cd.sh`, the contents of which will be:

        #!/bin/bash
        cd <wdir> || return

        ls ~


        This can then be run in parallel or singly:
        self.parallel(script)
        self.single(script)

        :param cmd: Command to write to file, or list of commands to write
        :param file_name: Name of file to create
        :return: Command to run script via plumbum interface
        """
        _path = os.path.join(self.wdir, file_name)
        fp = open(_path, "w")
        # Write shebang and move to working directory
        fp.write("#!/bin/bash\ncd %s || return\n\n" % self.wdir)
        # Write command to run
        if isinstance(cmd, list):
            for _cmd in cmd:
                fp.write("".join((str(_cmd), "\n")))
        else:
            fp.write("".join((str(cmd), "\n")))
        fp.close()
        self.local["chmod"]["+x", _path]()
        return self.local[_path]
