from typing import List

from src.result_map import ResultMap
from src.tasks.base_task import BaseTask
from src.utils.config_manager import ConfigManager


class Executor:
    def __init__(self, config_path: str):
        self.task_list: List[BaseTask] = []
        self.result_map: ResultMap = ResultMap()
        self.config_manager: ConfigManager = ConfigManager(config_path)

    def run(self):
        """

        :return:
        :rtype:
        """
        for task in self.task_list:
            self.result_map.distribute(task)
