from yapim.tasks.aggregate_task import AggregateTask
from yapim.tasks.task import Task
from yapim.tasks.task import TaskExecutionError, TaskSetupError
from yapim.tasks.task import TaskSetupError
from yapim.tasks.utils.dependency_input import DependencyInput
from yapim.tasks.utils.result import Result
from yapim.tasks.utils.version_info import VersionInfo
from yapim.utils.executor import Executor
from yapim.utils.extension_loader import ExtensionLoader
from yapim.utils.helpers import touch, prefix
from yapim.utils.input_loader import InputLoader

# TODO: Conditionally run tasks based on evaluating condition
# TODO: I.e., run a task for a set of input, but not for another set of same input
