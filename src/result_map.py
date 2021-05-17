from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import List, Type

from src.tasks.task import Task
from src.tasks.utils.dependency_graph import Node
from src.utils.config_manager import ConfigManager
from src.utils.path_manager import PathManager
from src.utils.result import Result


class ResultMap(dict):
    def __init__(self, config_manager: ConfigManager):
        super().__init__()
        self.config_manager = config_manager

    def distribute(self, task: Type[Task], task_identifier: Node, path_manager: PathManager):
        """

        :param task:
        :type task:
        :param task_identifier:
        :type task_identifier:
        :param path_manager:
        :type path_manager:
        :return:
        :rtype:
        """
        workers = self.config_manager.parent_info(task_identifier.get())[ConfigManager.WORKERS]
        with ThreadPoolExecutor(workers) as executor:
            futures: List[Future] = []
            for record_id, record_data in self.items():
                wdir = "_".join(task_identifier.get())
                path_manager.add_dirs(record_id, [wdir])
                task_copy = task(
                    record_id,
                    task_identifier.scope,
                    self,
                    path_manager.get_dir(record_id, wdir)
                )
                task_copy.input = record_data
                self._update_input(task_copy)
                futures.append(executor.submit(task.run_task))

            for future in as_completed(futures):
                result: Result = future.result()
                if result.record_id not in self.keys():
                    self[result.record_id] = {}
                self[result.record_id][result.task_name] = result

    def _update_input(self, task_copy: Task):
        for dependency in task_copy.depends:
            for prior in dependency.collect_all:
                task_copy.input[prior] = self[task_copy.record_id][prior]
        for dependency in task_copy.depends:
            for prior in dependency.collect_by:
                for prior_id, prior_mapping in prior:
                    for _from, _to in prior_mapping.items():
                        task_copy.input[_to] = self[task_copy.record_id][prior_id][_from]
