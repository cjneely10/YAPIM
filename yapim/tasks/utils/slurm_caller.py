"""
Module holds logic for running a dask distributed task within a SLURM job
"""
import os
import random
from pathlib import Path
from time import sleep
from typing import List, Union, Optional

from plumbum.machines.local import LocalCommand, local

from yapim.tasks.utils.slurm_status import SlurmStatus
from yapim.utils.config_manager import ConfigManager


class SlurmRunError(Exception):
    pass


class SLURMCaller:
    """ SLURMCaller handles running a program on a SLURM cluster

    Overrides __str__ and __call__ method to allow script command to be displayed within config
    and called by dask task manager, respectively.

    """
    OUTPUT_SCRIPTS = "slurm-runner.sh"
    FAILED_ID = "failed-job-id"
    status: Optional[SlurmStatus] = None

    def __init__(self,
                 cmd: Union[LocalCommand, str, List[Union[LocalCommand, str]]],
                 task,
                 time_override: Optional[str] = None,
                 parallelize: bool = False
                 ):
        """ Generate SLURMCaller object using user metadata gathered from SLURM config section and
        the task's own metadata
        """
        self.task = task
        self.cmd = cmd
        self.parallelize = parallelize
        self.config_manager = task.config_manager
        self.user_id = self.config_manager.get_slurm_userid()
        self.time_override = time_override
        if SLURMCaller.status is None:
            SLURMCaller.status = SlurmStatus(self.user_id)

        # Generated job id
        self.job_id: str = SLURMCaller.FAILED_ID
        # Initialize as not running
        self.running = False
        # Path of script that will run
        self.script = str(os.path.join(task.wdir, SLURMCaller.OUTPUT_SCRIPTS))
        # Create slurm script in working directory
        self._generate_script()

    def __str__(self) -> str:
        """ Return contents of script as a string

        :return: Contents of SLURM script
        """
        return open(self.script, "r").read()

    def __repr__(self) -> str:
        """ REPL version of string

        :return: Contents of SLURM script
        """
        return self.__str__()

    def _launch_script(self):
        """ Run generated script using sbatch

        Check if loaded properly and store in object data if properly launched
        """
        # Call script using sbatch
        log_line = str(local["sbatch"][self.script]()).split()
        # If no output to stdout/err, return
        if len(log_line) == 0:
            return
        # Otherwise set running status based on contents of stdout/err
        self.running = self._has_launched(log_line[-1])

    # Call squeue using user id and check if created job id is present
    def _is_running(self) -> bool:
        """ Method will check if the job_id created by launching the sbatch script is still
        visible within a user's `squeue`.

        :return: Task still running (true) or has completed/failed to start (false)
        """
        return self.running and SLURMCaller.status.check_status(self.job_id)

    def _has_launched(self, log_line: str) -> bool:
        """ Parse output from sbatch to see if job id was adequately created

        :param log_line: Output from running sbatch that should be a parsable integer
        :return: True if launched properly, or False if unable to parse/task was not launched properly
        """
        try:
            self.job_id = str(int(log_line))
            SLURMCaller.status.update()
        except ValueError:
            return False
        return True

    def _generate_script(self):
        """ Create script using passed command within SLURM format

        Include all additional flags to SLURM

        Flags to command should already be passed to command itself
        """
        # Initialize file
        file_ptr = open(self.script, "w")
        file_ptr.write("#!/bin/bash\n\n")

        # Write all header lines
        nodes = self.config_manager.find(self.task.full_name, ConfigManager.NODES)
        if nodes is None:
            nodes = "1"
        file_ptr.write(SLURMCaller._create_header_line("--nodes", nodes))

        tasks = self.config_manager.find(self.task.full_name, ConfigManager.TASKS)
        if tasks is None:
            tasks = "1"
        file_ptr.write(SLURMCaller._create_header_line("--tasks", tasks))

        file_ptr.write(
            SLURMCaller._create_header_line("--cpus-per-task",
                                            self.config_manager.find(self.task.full_name, ConfigManager.THREADS))
        )
        file_ptr.write(
            SLURMCaller._create_header_line("--mem",
                                            str(self.config_manager.find(self.task.full_name, ConfigManager.MEMORY)) +
                                            "GB")
        )
        file_ptr.write(
            SLURMCaller._create_header_line("--time",
                                            self.config_manager.find(self.task.full_name, ConfigManager.TIME)
                                            if self.time_override is None else self.time_override)
        )
        # Write additional header lines passed in by user
        for added_arg in self.config_manager.get_sbatch_flagged_arguments():
            file_ptr.write(SLURMCaller._create_header_line(*added_arg))
        file_ptr.write("\n")

        added_header = self.config_manager.find(self.task.full_name, ConfigManager.SLURM_HEADER)
        if isinstance(added_header, list):
            for header_line in added_header:
                file_ptr.write(header_line)
                file_ptr.write("\n")
            file_ptr.write("\n")
        # Write command to run
        if self.parallelize:
            ending_string = " &\n"
        else:
            ending_string = "\n"
        if isinstance(self.cmd, list):
            for cmd in self.cmd:
                file_ptr.write(str(cmd) + ending_string)
        else:
            file_ptr.write("".join((str(self.cmd), "\n")))
        if self.parallelize:
            file_ptr.write("wait\n")
        file_ptr.close()

    @staticmethod
    def _create_header_line(param: str, arg: str) -> str:
        """ Creates a header line in a SLURM script of form SBATCH key=val

        :param param: key
        :param arg: val
        :return: str of header line (with newline added)
        """
        return "#SBATCH %s=%s\n" % (param, arg)

    @property
    def slurm_log_file(self) -> str:
        return "slurm-%s.out" % self.job_id

    def __call__(self, *args, **kwargs):
        """ Call will run script using sbatch and periodically check for when to stop running

        :param args: Any args passed
        :param kwargs: Any kwargs passed
        """
        self._launch_script()
        sleep(47 + random.randint(1, 11))  # Wait 1 minute in between checking if still running
        while self._is_running():
            print(f"{self.task.name} waiting for {self.task.record_id}")
            sleep(47 + random.randint(1, 11))  # Wait 1 minute in between checking if still running
        slurm_file = Path("slurm-%s.out" % self.job_id)
        if slurm_file.exists():
            _file = "\n".join(open(slurm_file, "r").readlines())
            if "ERROR" in _file and "TIME" in _file:
                raise SlurmRunError("Timeout found in SLURM job")
