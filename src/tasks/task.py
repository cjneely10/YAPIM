from abc import ABC
from pathlib import Path

from plumbum import local
from plumbum.machines import LocalMachine, LocalCommand

from src.tasks.base_task import BaseTask
from src.tasks.utils.result import Result
from src.utils.config_manager import ConfigManager


class Task(BaseTask, ABC):
    @property
    def task_scope(self) -> str:
        return self._task_scope

    def __init__(self, record_id: str, task_scope: str, result_map, wdir: str):
        self.record_id: str = record_id
        self._task_scope = task_scope
        self.input: dict = result_map[self.record_id]
        self.output = {}
        self.wdir: Path = Path(wdir).resolve()
        self.config: dict = result_map.config_manager.get(self.full_name)
        parent_data = result_map.config_manager.parent_info(self.full_name)
        self.is_skip = "skip" in self.config.keys() and self.config["skip"] is True or \
                       "skip" in parent_data.keys() and parent_data["skip"] is True
        self.is_complete = False

    def run_task(self) -> Result:
        """ Type of run. For Task objects, this simply calls run(). For other tasks, there
        may be more processing required prior to returning the result.

        This method will be used to return the result of the child Task class implemented run method.

        :return:
        """
        if self.is_skip:
            return Result(self.record_id, self.task_name, {})
        if not self.is_complete:
            self.run()
        for key, output in self.output.items():
            if isinstance(output, Path) and not output.exists():
                raise BaseTask.TaskCompletionError(key, output)
        return Result(self.record_id, self.task_name, self.output)

    @property
    def local(self) -> LocalMachine:
        return local

    @property
    def program(self) -> LocalCommand:
        return self.local[self.config[ConfigManager.PROGRAM]]
