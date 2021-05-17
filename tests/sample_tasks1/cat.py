from src.tasks.task import Task, set_complete, program_catch
from src.tasks.utils.dependency_input import DependencyInput


class Print(Task):
    task_name = "cat"
    requires = ["echo"]
    depends = [DependencyInput("sed", {"echo": {"result": "file"}})]

    @set_complete
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.input["sed"]["result"],
            "final": ["result"]
        }

    @program_catch
    def run(self):
        """

        :return:
        :rtype:
        """
