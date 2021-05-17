from typing import List, Dict

import networkx as nx

from src.tasks.base_task import BaseTask

# TODO: Link in with created dependencies from EukMS, refactored for new protocol
dependencies = {}


class DependencyGraphGenerationError(BaseException):
    """ Exception class if unable to form directed acyclic (dependency) graph from
    provided pipeline configuration

    """
    pass


class DependencyGraph:
    """ Class takes list of tasks and creates dependency DAG of data
        Topological sort outputs order in which tasks can be completed

        All tasks are assumed to require (at least) a root task to complete

        The root task is simply populating input files to run in a pipeline

        """
    ERR = DependencyGraphGenerationError(
        "There was an error generating dependency graph information for this pipeline.\n"
        "If you are a user seeing this, this is likely a fatal error - contact the authors of the pipeline\n"
        "If you are a developer, double-check dependency information in your pipeline"
    )

    ROOT = "input"

    def __init__(self, tasks: List[BaseTask]):
        """ Create DAG from list of TaskList class objects

        :param tasks: List of TaskList class objects
        :raises: DependencyGraphGenerationError
        """
        self.idx: Dict[str, BaseTask] = {task.task_name: task for task in tasks}

        self.idx.update(dependencies)

        self.graph = nx.DiGraph()
        self.graph.add_node(DependencyGraph.ROOT)
        self._build_dependency_graph(tasks)
        if not nx.is_directed_acyclic_graph(self.graph):
            raise DependencyGraph.ERR

    def _build_dependency_graph(self, tasks: List[BaseTask]):
        for task in tasks:
            self.graph.add_edge(DependencyGraph.ROOT, task.task_name)
            # Add to index of tasks if not already present
            # Link requirements for already completed tasks in pipeline
            for requirement in task.requires:
                if requirement not in self.idx.keys():
                    raise DependencyGraph.ERR
                self.graph.add_edge(task.task_name, requirement)
            # Gather dependencies needed for fulfilling given requirement
            self._add_dependencies(task.task_name)

    def _add_dependencies(self, task_name: str):
        # Link dependency names
        for dependency in self.idx[task_name].depends:
            if dependency not in self.idx.keys():
                raise DependencyGraph.ERR
            self.graph.add_edge(task_name, dependency.name)
            for name in dependency.all_priors():
                self.graph.add_edge(task_name, name)
            self._add_dependencies(dependency.name)

    @property
    def sorted_graph(self) -> List[BaseTask]:
        return [self.idx[task_name] for task_name in nx.topological_sort(self.graph)]
