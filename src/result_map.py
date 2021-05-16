from src.tasks.task import Task


class ResultMap(dict):
    def __init__(self):
        super().__init__()

    def distribute(self, task: Task):
        """

        :param task:
        :type task:
        :return:
        :rtype:
        """
