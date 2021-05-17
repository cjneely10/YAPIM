from typing import List, Dict, Tuple, Type

from networkx import DiGraph, topological_sort, is_directed_acyclic_graph

from src.tasks.task import Task

# TODO: Link in with created dependencies from EukMS, refactored for new protocol
dependencies = {}


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
    ROOT_NODE = Node(ROOT, ROOT)

    def __init__(self, tasks: List[Type[Task]]):
        """ Create DAG from list of TaskList class objects

        :param tasks: List of TaskList class objects
        :raises: DependencyGraphGenerationError
        """
        self.idx: Dict[str, Type[Task]] = {task.task_name: task for task in tasks}
        self.idx.update(dependencies)

        self.graph = DiGraph()
        self.graph.add_node(DependencyGraph.ROOT_NODE)
        self._build_dependency_graph(tasks)
        if not is_directed_acyclic_graph(self.graph):
            raise DependencyGraph.ERR

    def _build_dependency_graph(self, tasks: List[Type[Task]]):
        for task in tasks:
            task_node: Node = Node(DependencyGraph.ROOT, task.task_name)
            self.graph.add_edge(DependencyGraph.ROOT_NODE, task_node)
            # Link requirements for already completed tasks in pipeline
            # Gather dependencies needed for fulfilling given requirement
            self._add_dependencies(task_node)

    def _add_dependencies(self, task_node: Node):
        # Link dependency names
        task = self.idx[task_node.name]
        for requirement in task.requires:
            if requirement not in self.idx.keys():
                raise DependencyGraph.ERR
            self.graph.add_edge(task_node, Node(DependencyGraph.ROOT, requirement))

        for dependency in task.depends:
            if dependency not in self.idx.keys():
                raise DependencyGraph.ERR
            dependency_node: Node = Node(task_node.name, dependency.name)
            self.graph.add_edge(task_node, dependency_node)
            for name in dependency.all_priors():
                self.graph.add_edge(task_node, name)
            self._add_dependencies(dependency_node)

    @property
    def sorted_graph_identifiers(self) -> List[Node]:
        return list(topological_sort(self.graph))[1:]
