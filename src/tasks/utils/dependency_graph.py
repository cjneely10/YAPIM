from typing import List, Dict, Tuple, Type

from networkx import DiGraph, topological_sort, is_directed_acyclic_graph

from src.tasks.task import Task
from src.tasks.utils.dependency_input import DependencyInput
from src.utils.config_manager import ConfigManager


class DependencyGraphGenerationError(BaseException):
    """ Exception class if unable to form directed acyclic (dependency) graph from
    provided pipeline configuration

    """
    pass


class Node:
    def __init__(self, task_scope: str, task_name: str):
        self.scope = task_scope
        self.name = task_name

    def get(self) -> Tuple[str, str]:
        return self.scope, self.name

    def __str__(self):
        return f"{self.scope} {self.name}"

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self.scope + "." + self.name)

    def __eq__(self, other: "Node"):
        if not isinstance(other, Node):
            return False
        return self.scope == other.scope and self.name == other.name


class DependencyGraph:
    """ Class takes list of tasks and creates dependency DAG of data
        Topological sort outputs order in which tasks can be completed

        All tasks are assumed to require (at least) a root task to complete

        The root task is simply populating input files to run in a pipeline

        """
    ERR = DependencyGraphGenerationError(
        "There was an error generating dependency graph information for this pipeline.\n"
        "If you are a user seeing this, this is likely a fatal error - contact the authors of the pipeline\n"
        "If you are a developer, double-check dependency information in your pipeline and ensure all ids are valid \n"
        "across the config file and Task file setup"
    )

    ROOT = ConfigManager.ROOT
    ROOT_NODE = Node(ROOT, ROOT)

    def __init__(self, tasks: List[Type[Task]], task_blueprints: Dict[str, Type[Task]]):
        """ Create DAG from list of TaskList class objects

        :param tasks: List of TaskList class objects
        :raises: DependencyGraphGenerationError
        """
        self.idx: Dict[str, Type[Task]] = task_blueprints

        self.graph = DiGraph()
        self.graph.add_node(DependencyGraph.ROOT_NODE)
        self._build_dependency_graph(tasks)
        if not is_directed_acyclic_graph(self.graph):
            raise DependencyGraph.ERR

    def _build_dependency_graph(self, tasks: List[Type[Task]]):
        for task in tasks:
            task_node: Node = Node(DependencyGraph.ROOT, task.__name__)
            self.graph.add_edge(DependencyGraph.ROOT_NODE, task_node)
            # Link requirements for already completed tasks in pipeline
            # Gather dependencies needed for fulfilling given requirement
            for requirement in task.requires():
                if requirement not in self.idx.keys():
                    raise DependencyGraph.ERR
                self.graph.add_edge(Node(DependencyGraph.ROOT, requirement), task_node)

    def _add_dependencies(self, task_node: Node, step_list: list):
        # Link dependency names
        task = self.idx[task_node.name]

        dependency: DependencyInput
        for dependency in task.depends():
            if dependency.name not in self.idx.keys():
                raise DependencyGraph.ERR
            dependency_node: Node = Node(task_node.name, dependency.name)
            step_list.append(dependency_node)
            self.graph.add_edge(task_node, dependency_node)
            self._add_dependencies(dependency_node, step_list)

    @property
    def sorted_graph_identifiers(self) -> List[List[Node]]:
        sorted_graph = list(topological_sort(self.graph))
        sorted_graph.remove(DependencyGraph.ROOT_NODE)
        out_steps = []
        for node in sorted_graph:
            step_list = []
            self._add_dependencies(node, step_list)
            step_list.reverse()
            out_steps.append([*step_list, node])
        return out_steps
