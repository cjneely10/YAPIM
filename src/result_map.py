from copy import deepcopy
from typing import List

from src.tasks.base_task import BaseTask
from src.utils.result import Result
from concurrent.futures import ThreadPoolExecutor, as_completed, Future


class ResultMap(dict):
    def __init__(self):
        super().__init__()

    def distribute(self, task: BaseTask):
        """

        :param task:
        :type task:
        :return:
        :rtype:
        """
        with ThreadPoolExecutor() as executor:
            futures: List[Future] = []
            for record_id, record_data in self.items():
                task_copy = deepcopy(task)
                task_copy.input = record_data
                task_copy.record_id = record_id
                futures.append(executor.submit(task.run_task))

            for future in as_completed(futures):
                result: Result = future.result()
                if result.record_id not in self.keys():
                    self[result.record_id] = {}
                self[result.record_id][result.task_name] = result

