from abc import ABC
from pathlib import Path

from src.result_map import ResultMap
from src.tasks.base_task import BaseTask
from src.utils.result import Result


class Task(BaseTask, ABC):
    @property
    def task_scope(self) -> str:
        return self._task_scope

    def __init__(self, record_id: str, task_scope: str, result_map: ResultMap, wdir: str):
        self.record_id = record_id
        self._task_scope = task_scope
        self.input = result_map[self.record_id][self.full_name]
        self.output = {}
        self.wdir = wdir
        self.config = result_map.config_manager.get(self.full_name)

    def run_task(self) -> Result:
        """ Type of run. For Task objects, this simply calls run(). For other tasks, there
        may be more processing required prior to returning the result.

        This method will be used to return the result of the child Task class implemented run method.

        :return:
        """
        for key, output in self.output:
            if isinstance(output, Path) and not output.exists():
                raise BaseTask.TaskCompletionError(key, output)

        return Result(self.record_id, self.task_name, self.output)
