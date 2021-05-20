import unittest
from typing import Type, Iterable, Dict, Tuple

from HPCBioPipe.utils.config_manager import ConfigManager
from HPCBioPipe.utils.dependency_graph import DependencyGraph, Node, DependencyGraphGenerationError
from tests.dependency_graph.class_stubs import *

TaskType = Type[Task]


def generate_dg_input(task_list: Iterable[TaskType]) -> Tuple[List[TaskType], Dict[str, TaskType]]:
    return list(task_list), {task.__name__: task for task in task_list}


class TestDependencyGraph(unittest.TestCase):
    def test_empty(self):
        self.assertEqual([], DependencyGraph(*generate_dg_input([])).sorted_graph_identifiers)

    def test_single(self):
        self.assertEqual([
            [Node(ConfigManager.ROOT, A.__name__)]
        ],
            DependencyGraph(*generate_dg_input([A])).sorted_graph_identifiers)

    def test_simple_expects(self):
        self.assertEqual([
            [Node(ConfigManager.ROOT, A.__name__)],
            [Node(ConfigManager.ROOT, B.__name__)],
            [Node(ConfigManager.ROOT, C.__name__)],
        ],
            DependencyGraph(*generate_dg_input([A, B, C])).sorted_graph_identifiers)

    def test_simple_depends(self):
        self.assertEqual([
            [Node(ConfigManager.ROOT, A.__name__)],
            [Node(ConfigManager.ROOT, B.__name__)],
            [Node(E.__name__, C.__name__), Node(ConfigManager.ROOT, E.__name__)],
            [Node(ConfigManager.ROOT, C.__name__)],
        ],
            DependencyGraph(*generate_dg_input([A, B, C, E])).sorted_graph_identifiers)

    def test_unmet_expects(self):
        with self.assertRaises(DependencyGraphGenerationError):
            self.fail()

    def test_unmet_depends(self):
        with self.assertRaises(DependencyGraphGenerationError):
            self.fail()

    def test_cyclic(self):
        with self.assertRaises(DependencyGraphGenerationError):
            self.fail()

    def test_circular(self):
        with self.assertRaises(DependencyGraphGenerationError):
            self.fail()

    def test_bad_requires(self):
        with self.assertRaises(DependencyGraphGenerationError):
            self.fail()

    def test_bad_depends(self):
        with self.assertRaises(DependencyGraphGenerationError):
            self.fail()


if __name__ == '__main__':
    unittest.main()
