import logging
import os
import shutil
import threading
import time
import traceback
from abc import ABC
from pathlib import Path
from typing import Tuple, List, Union, Optional, Callable, Iterable

from plumbum import local, colors, ProcessExecutionError
from plumbum.machines import LocalMachine, LocalCommand

from yapim.tasks.utils.base_task import BaseTask
from yapim.tasks.utils.input_dict import InputDict
from yapim.tasks.utils.slurm_caller import SLURMCaller
from yapim.tasks.utils.task_result import TaskResult
from yapim.tasks.utils.version_info import VersionInfo
from yapim.utils.config_manager import ConfigManager, MissingDataError, MissingProgramSection


class TaskSetupError(AttributeError):
    """
    Wrap AttributeError around improperly-formatted Task fields
    """


class TaskExecutionError(RuntimeError):
    """
    Wrapper for error within running a task associated with an error in config lookup or other utility- or setup-
    related issues
    """


def clean(*directories: Union[Path, str]):
    def method(func: Callable):
        def fxn(self):
            for directory in directories:
                out_dir = self.wdir.joinpath(directory)
                if out_dir.exists():
                    shutil.rmtree(out_dir)
            func(self)

        return fxn

    return method


# TODO: Parser to identify illegal self.input accesses and prevent pipeline launch at start time
# https://stackoverflow.com/questions/43166571/getting-all-the-nodes-from-python-ast-that-correspond-to-a-particular-variable-w
class Task(BaseTask, ABC):
    print_lock = threading.Lock()

    def __init__(self,
                 record_id: str,
                 task_scope: str,
                 config_manager: ConfigManager,
                 input_data: dict,
                 added_data: dict,
                 wdir: str,
                 display_messages: bool):
        self._record_id = record_id
        self._task_scope = str(task_scope)
        # input_data.update(added_data)
        # self.input = input_data
        # added_data.update(input_data)
        self.input = input_data.copy()
        self.input.update(added_data)
        self.input = InputDict(self.input)
        # print(self.input)
        # print(added_data)
        self.output = {}
        self.wdir: Path = Path(wdir).resolve()
        self.config_manager = config_manager
        is_skip = self.config_manager.find(self.full_name, ConfigManager.SKIP)
        if is_skip is not None and str(is_skip).lower() == "true":
            self.is_skip = True
        else:
            self.is_skip = False
        self.is_complete = False
        self.display_messages = display_messages
        self._versions = self.get_versions()

    @property
    def record_id(self) -> str:
        return self._record_id

    def versions(self) -> List[VersionInfo]:
        pass

    def condition(self) -> bool:
        pass

    def has_run(self, task_name: str, record_id: Optional[str] = None):
        return task_name in self.input.keys()

    def get_versions(self) -> Optional[List[str]]:
        """ Get version of program that Task is currently running

        :return:
        :rtype:
        """
        versions = self.versions()
        if ConfigManager.PROGRAM not in self.config.keys() or versions is None or len(versions) == 0:
            return None
        out_versions = []
        for version in versions:
            try:
                if isinstance(version, VersionInfo):
                    if version.config_param is None:
                        response = self.program[version.calling_parameter]()
                    else:
                        response = self.local[self.config[version.config_param]][version.calling_parameter]()
                    if version.version in response:
                        out_versions.append(version.version)
                else:
                    raise AttributeError("Versions must be of type VersionInfo")
            except ProcessExecutionError:
                continue
        if len(out_versions) != 0:
            return out_versions
        raise TaskExecutionError(
            f"{self.name} was launched using a version that does not have a defined implementation"
        )

    def task_scope(self) -> str:
        return self._task_scope

    @property
    def storage_directory(self) -> Path:
        return self.config_manager.storage_directory

    @property
    def threads(self) -> str:
        """ Number of threads when running task (as set in config file)

        :return: Str of number of tasks
        """
        return self.config_manager.find(self.full_name, ConfigManager.THREADS)

    @property
    def memory(self) -> str:
        """ Number of threads when running task (as set in config file)

        :return: Str of number of tasks
        """
        return self.config_manager.find(self.full_name, ConfigManager.MEMORY)

    @property
    def config(self) -> dict:
        return self.config_manager.get(self.full_name)

    @property
    def added_flags(self) -> List[str]:
        """ Get additional flags that user provided in config file

        Example: self.local["ls"][(*self.added_flags("ls"))]

        :return: List of arguments to pass to calling program
        """
        return self.flags_to_list(ConfigManager.FLAGS)

    def flags_to_list(self, config_param: str):
        """ Get additional flags from given section, parsed to list

        Example: self.local["ls"][(*self.added_flags("ls"))]

        :return: List of arguments to pass to calling program
        """
        return ConfigManager.flags_to_list(self.config_manager, self, config_param)

    @property
    def is_slurm(self) -> bool:
        """
        Run was launched on SLURM
        """
        return bool(self.config_manager.config[ConfigManager.SLURM][ConfigManager.USE_CLUSTER])

    def _create_slurm_command(self,
                              cmds: Union[LocalCommand, List[LocalCommand]],
                              time_override: Optional[str] = None,
                              parallelize: bool = False) -> SLURMCaller:  # pragma: no cover
        """ Create a SLURM-managed process

        :param cmd: plumbum LocalCommand object to run
        :return: SLURM-wrapped command to run script via plumbum interface
        """
        # Confirm valid SLURM section
        parent_info: dict = self.config_manager.parent_info(self.full_name)
        if ConfigManager.MEMORY not in self.config.keys() and ConfigManager.MEMORY not in parent_info.keys():
            raise MissingDataError("SLURM section not properly formatted within %s" % str(self.full_name))
        if ConfigManager.TIME not in self.config.keys() and ConfigManager.TIME not in parent_info.keys():
            raise MissingDataError("SLURM section not properly formatted within %s" % str(self.full_name))
        # Generate command to launch SLURM job
        return SLURMCaller(cmds, self, time_override, parallelize)

    @property
    def data(self) -> List[str]:
        """ List of data files passed to this task's config section

        :return: List of paths of data in task's config section
        """
        return self.config[ConfigManager.DATA].split(" ")

    def run_task(self) -> TaskResult:
        """ Type of run. For Task objects, this simply calls run(). For other tasks, there
        may be more processing required prior to returning the result.

        This method will be used to return the result of the child Task class implemented run method.

        :return:
        """
        # Conditional run - either undefined (in which case self.skip defined by config presence/definition)
        # Or defined by result of evaluating condition
        condition = self.condition()
        if condition is not None:
            if condition is False:
                self.is_skip = True
        if self.is_skip:
            return TaskResult(self.record_id, self.name, self.output)

        if not self.is_complete:
            with Task.print_lock:
                if self.display_messages:
                    print(colors.green & colors.bold | "\nRunning:\n  %s" % (
                            (self.task_scope() + " " if self.task_scope() != ConfigManager.ROOT else "")
                            + (self.name if self.task_scope() == ConfigManager.ROOT else f"(using {self.name})")
                    ))
                _str = "In progress:  {}".format(str(self.record_id))
                logging.info(_str)
                if self.display_messages:
                    print(colors.blue & colors.bold | _str)
            start_time = time.time()
            self.try_run()
            end_time = time.time()
            with Task.print_lock:
                _str = "Is complete:  {} ({:.3f}{})".format(str(self.record_id),
                                                            *Task._parse_time(end_time - start_time))
                logging.info(_str)
                if self.display_messages:
                    print(colors.blue & colors.bold | _str)

        for key, output in self.output.items():
            if key != "final":
                if (isinstance(output, Path) and not output.exists()) or \
                        (isinstance(output, str) and not os.path.exists(output)):
                    raise super().TaskCompletionError(self.name, key, Path(output))
        return TaskResult(self.record_id, self.name, self.output)

    @property
    def local(self) -> LocalMachine:
        return local

    @property
    def program(self) -> LocalCommand:
        program = self.config_manager.find(self.full_name, ConfigManager.PROGRAM)
        if program is None:
            raise MissingProgramSection(f"Program key not set in config section for {self.full_name}")
        return self.local[program]

    def __str__(self):  # pragma: no cover
        return f"<Task name: {self.name}, scope: {self.task_scope()}, input_id: {str(self.record_id)}, " \
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
            raise err

    @staticmethod
    def _parse_time(_time: float) -> Tuple[float, str]:  # pragma: no cover
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
                else:
                    is_complete = True
        if is_complete is None:
            is_complete = False
        self.is_complete = is_complete

    def parallel(self, cmd: LocalCommand, time_override: Optional[str] = None):
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
        :param time_override:
        :raises: MissingDataError if SLURM section improperly configured
        """
        # Write command to slurm script file and run
        if self.is_slurm:
            cmd = self._create_slurm_command(cmd, time_override)
        # Run command directly
        logging.info(str(cmd))
        if self.display_messages:
            print("  " + str(cmd))
        with open(os.path.join(self.wdir, "task.log"), "a") as w:
            w.write(str(cmd) + "\n")
        out = cmd()
        # Store log info in any was generated
        if out is not None:
            with open(os.path.join(self.wdir, "task.log"), "a") as w:
                w.write(str(out) + "\n")
        if isinstance(cmd, SLURMCaller) and os.path.exists(cmd.slurm_log_file):
            with open(os.path.join(self.wdir, "task.log"), "a") as w:
                w.write("------BEGIN SLURM LOG OUTPUT SECTION------\n")
                w.write("".join(open(cmd.slurm_log_file, "r").readlines()))
                w.write("------END SLURM LOG OUTPUT SECTION------\n")
        with open(os.path.join(self.wdir, "task.log"), "a") as w:
            w.write("\n")

    def _iter_batch(self, cmds: Iterable[LocalCommand]):
        threads = int(self.threads)
        while True:
            out = []
            try:
                i = 0
                while i < threads:
                    out.append(next(cmds))
                    i += 1
                yield out
            except StopIteration:
                yield out

    # Current behaviour: Generates batches of commands of size self.threads, runs each batch with full resources
    # listed in SLURM section. Since we are generating commands of size self.threads, this makes sense, and since
    # batches will run serially, we are not over-extending resource allocations
    def batch(self, cmds: Iterable[LocalCommand]):
        for i, cmd_batch in enumerate(self._iter_batch(cmds)):
            if self.is_slurm:
                # Okay since will be immediately re-written as slurm
                self.parallel(cmd_batch)
            else:
                self.parallel(
                    self.create_script(cmd_batch, f"batch-{i}.sh", parallelize=True)
                )

    def single(self, cmd: LocalCommand, time_override: Optional[str] = None):
        """ Launch a command that uses a single thread.

        The command string will be written to the EukMetaSanity pipeline output file and will be printed to screen

        Example:
        self.single(self.local["pwd"])

        :param cmd: plumbum LocalCommand object to run, or list of commands to run
        :param time_override:
        """
        self.parallel(cmd, time_override)

    def create_script(self, cmd: Union[str, LocalCommand, List[Union[str, LocalCommand]]], file_name: str,
                      parallelize: bool = False) \
            -> LocalCommand:
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
                fp.write("".join((str(_cmd), "%s\n" % (" &" if parallelize else ""))))
        else:
            fp.write("".join((str(cmd), "\n")))
        if parallelize:
            fp.write("wait\n")
        fp.close()
        self.local["chmod"]["+x", _path]()
        return self.local[_path]

    @staticmethod
    def finalize(obj_results: dict, class_results: dict, task: "Task", result: TaskResult) -> dict:
        class_results[result.record_id][result.task_name] = result
        obj_results[result.task_name] = result
        return class_results
