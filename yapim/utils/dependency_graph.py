import inspect
from typing import List, Dict, Tuple, Type, Iterable, Optional, Set

import networkx as nx

from yapim import AggregateTask
from yapim.tasks.task import Task
from yapim.tasks.utils.dependency_input import DependencyInput
from yapim.utils.config_manager import ConfigManager


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

    def __str__(self):  # pragma: no cover
        return f"<Node scope: {self.scope}, name: {self.name}>"

    def __repr__(self):  # pragma: no cover
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

        self._graph = nx.DiGraph()
        self._graph.add_node(DependencyGraph.ROOT_NODE)
        self._build_dependency_graph(tasks)
        self._sorted_graph: Optional[List[List[Node]]] = None
        if not nx.is_directed_acyclic_graph(self._graph):
            print("Cycle found!")
            raise DependencyGraph.ERR
        self.sort_graph()

    def _build_dependency_graph(self, tasks: Iterable[Type[Task]]):
        for task in tasks:
            task_node: Node = Node(DependencyGraph.ROOT, task.__name__)
            self._graph.add_edge(DependencyGraph.ROOT_NODE, task_node)
            # Link requirements for already completed tasks in pipeline
            # Gather dependencies needed for fulfilling given requirement
            if task.requires() is None:
                continue
            if not isinstance(task.requires(), list):
                print("requires() must return a list")
                raise DependencyGraph.ERR
            for requirement in task.requires():
                if not isinstance(requirement, (str, type)):
                    print("Requirements must be strings or class objects")
                    raise DependencyGraph.ERR
                # Can pass in requirements by string or by type
                if inspect.isclass(requirement):
                    requirement = requirement.__name__
                if requirement not in self.idx.keys():
                    print(f"Unable to locate {requirement}")
                    raise DependencyGraph.ERR
                self._graph.add_edge(Node(DependencyGraph.ROOT, requirement), task_node)

    def _add_dependencies(self, task_node: Node, scope: str, graph: nx.Graph):
        # Link dependency names
        task = self.idx[task_node.name]
        dependency: DependencyInput
        if task.depends() is None:
            return
        if not isinstance(task.depends(), list):
            print("depends() must return a list")
            raise DependencyGraph.ERR
        for dependency in task.depends():
            if not isinstance(dependency, DependencyInput) or not isinstance(dependency.name, (str, type)):
                print("Dependency names must be strings or class objects")
                raise DependencyGraph.ERR
            if inspect.isclass(dependency.name):
                dependency.name = dependency.name.__name__
            if dependency.name not in self.idx.keys():
                print(f"Unable to locate {dependency.name}")
                raise DependencyGraph.ERR
            scope_type = self.idx[dependency.name]
            if issubclass(scope_type, AggregateTask):
                if not issubclass(task, AggregateTask):
                    print(
                        f"Task <{task.__name__}> is of type AggregateTask and can only use AggregateTask dependencies"
                    )
                    raise DependencyGraph.ERR
            else:
                if issubclass(task, AggregateTask):
                    print(f"Task <{task.__name__}> is of type Task and cannot use AggregateTask dependencies")
                    raise DependencyGraph.ERR
            dependency_node: Node = Node(scope, dependency.name)
            graph.add_edge(task_node, dependency_node)
            self._add_dependencies(dependency_node, scope, graph)

    def sort_graph(self):
        sorted_graph = list(nx.topological_sort(self._graph))
        sorted_graph.remove(DependencyGraph.ROOT_NODE)
        out_steps = []
        for node in sorted_graph:
            graph = nx.DiGraph()
            self._add_dependencies(node, node.name, graph)
            step_list = list(reversed(list(nx.topological_sort(graph))))
            if len(step_list) > 0:
                out_steps.append(step_list)
            else:
                out_steps.append([node])
        self._sorted_graph = out_steps

    @property
    def sorted_graph_identifiers(self) -> List[List[Node]]:
        return self._sorted_graph

    def _find_task_idx(self, task_name: str, dependency_name: Optional[str] = None) -> Tuple[int, int]:
        for i, task_list in enumerate(self.sorted_graph_identifiers):
            if task_list[-1].name == task_name:
                if dependency_name is None:
                    return i, -1
                else:
                    for j, task in enumerate(task_list[:-1]):
                        if task.name == dependency_name:
                            return i, j
        return -1, -1

    def _get_tasks_name_strings(self, task_idx: Tuple[int, int]) -> List[str]:
        task_names: list = [self.sorted_graph_identifiers[task_idx[0]][-1].name]
        if task_idx[1] != -1:
            task_names.append(task_names[-1] + "." + self.sorted_graph_identifiers[task_idx[0]][task_idx[1]].name)
        return task_names

    def _connecting_paths(self, task1: Node, task2: Node):
        try:
            return next(nx.all_simple_paths(self._graph, task1, task2))
        except StopIteration:
            return []

    def get_affected_nodes(self, task_name: str, dependency_name: Optional[str] = None) -> Set[str]:
        out_nodes = set()
        self._get_nodes_helper(out_nodes, task_name, dependency_name)
        return out_nodes

    def _get_nodes_helper(self, out_nodes: Set[str], task_name: str, dependency_name: Optional[str] = None):
        task_pos = self._find_task_idx(task_name, dependency_name)
        if task_pos == (-1, -1):
            return
        for task_name in self._get_tasks_name_strings(task_pos):
            out_nodes.add(task_name)
        for i, task_list in enumerate(self.sorted_graph_identifiers[task_pos[0] + 1:]):
            connections = self._connecting_paths(self.sorted_graph_identifiers[task_pos[0]][-1], task_list[-1])
            if len(connections) > 0:
                self._get_nodes_helper(out_nodes, task_list[-1].name)
