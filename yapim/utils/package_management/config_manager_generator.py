from pathlib import Path
from typing import Type, Dict, List, Optional

from yapim import Task
from yapim.tasks.utils.loader import get_modules
from yapim.utils.dependency_graph import DependencyGraph, Node


# TODO: Load existing file and write fields as present
from yapim.utils.package_management.package_loader import PackageLoader


class ConfigManagerGenerator:
    def __init__(self, pipeline_module_path: Path, dependencies_directories: Optional[List[Path]]):
        pipeline_tasks, self.task_blueprints = PackageLoader.load_from_directories(pipeline_module_path,
                                                                                   dependencies_directories)
        self.task_list: List[List[Node]] = DependencyGraph(pipeline_tasks, self.task_blueprints) \
            .sorted_graph_identifiers

    def write(self, config_file_path: Path):
        with open(config_file_path, "w") as file_ptr:
            file_ptr.write("""---  # document start

###########################################
## Pipeline input section
INPUT:
  root: all

## Global settings
GLOBAL:
  # Maximum threads/cpus to use in analysis
  MaxThreads: 10
  # Maximum memory to use (in GB)
  MaxMemory: 100

###########################################

SLURM:
  ## Set to True if using SLURM
  USE_CLUSTER: false
  ## Pass any flags you wish below
  ## DO NOT PASS the following:
  ## --nodes, --ntasks, --mem, --cpus-per-task
  --qos: unlim
  --job-name: EukMS
  user-id: uid

""")
            task_node: Node
            for task_node_list in self.task_list:
                # Is single task, no dependencies
                if len(task_node_list) == 1:
                    file_ptr.write(ConfigManagerGenerator.task(task_node_list[-1]))
                    file_ptr.write("\n")
                # Task with dependencies to finish first
                else:
                    dependency_section = "dependencies:"
                    for dependency in task_node_list[:-1]:
                        dependency_section += f"""
    {dependency.name}:
      program:
"""
                    file_ptr.write(f'''{ConfigManagerGenerator.task(task_node_list[-1])}  {dependency_section}
''')

            file_ptr.write("""...  # document end""")
            file_ptr.close()

    @staticmethod
    def task(task_node: Node) -> str:
        return f'''{task_node.name}:
  # Number of threads task will use
  threads: 1
  # Amount of memory task will use (in GB)
  memory: 8
  time: "4:00:00"
'''
