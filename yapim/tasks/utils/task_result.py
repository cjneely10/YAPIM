class TaskResult(dict):
    """
    Wrapper for python dict class for Result of completing a Task on a given input set
    """
    def __init__(self, record_id: str, task_name: str, data: dict):
        super().__init__(data)
        self.record_id = record_id
        self.task_name = task_name
