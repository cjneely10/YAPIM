import os
import unittest
from pathlib import Path
from typing import Type, Iterable, Dict, Tuple, Union

from tests.dependency_graph.class_stubs import *
from yapim import AggregateTask
from yapim.utils.config_manager import ConfigManager
from yapim.utils.dependency_graph import DependencyGraph, Node, DependencyGraphGenerationError

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
            DependencyGraph(*generate_dg_input([B]))

    def test_unmet_depends(self):
        with self.assertRaises(DependencyGraphGenerationError):
            print(DependencyGraph(*generate_dg_input([A, B, E])).sorted_graph_identifiers)

    def test_cyclic(self):
        with self.assertRaises(DependencyGraphGenerationError):
            print(DependencyGraph(*generate_dg_input([AB, BA])).sorted_graph_identifiers)

    def test_bad_requires(self):
        class BadSetup(Task):
            @staticmethod
            def requires() -> List[Union[str, Type]]:
                pass

            @staticmethod
            def depends() -> List[DependencyInput]:
                return []

            def run(self):  # pragma: no cover
                pass

        with self.assertRaises(DependencyGraphGenerationError):
            print(DependencyGraph(*generate_dg_input([BadSetup])).sorted_graph_identifiers)

    def test_bad_requires2(self):
        class BadSetup(Task):
            @staticmethod
            def requires() -> List[Union[str, Type]]:
                return [1]

            @staticmethod
            def depends() -> List[DependencyInput]:
                return []

            def run(self):  # pragma: no cover
                pass

        with self.assertRaises(DependencyGraphGenerationError):
            print(DependencyGraph(*generate_dg_input([BadSetup])).sorted_graph_identifiers)

    def test_bad_depends_type(self):
        class BadSetup(Task):
            @staticmethod
            def requires() -> List[Union[str, Type]]:
                return []

            @staticmethod
            def depends() -> List[DependencyInput]:
                pass

            def run(self):  # pragma: no cover
                pass

        with self.assertRaises(DependencyGraphGenerationError):
            print(DependencyGraph(*generate_dg_input([BadSetup])).sorted_graph_identifiers)

    def test_bad_depends_type2(self):
        class BadSetup(Task):
            @staticmethod
            def requires() -> List[Union[str, Type]]:
                return []

            @staticmethod
            def depends() -> List[DependencyInput]:
                return ["str"]

            def run(self):  # pragma: no cover
                pass

        with self.assertRaises(DependencyGraphGenerationError):
            print(DependencyGraph(*generate_dg_input([BadSetup])).sorted_graph_identifiers)

    def test_bad_aggregate(self):
        class BadAggregate(AggregateTask):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

            def aggregate(self) -> dict:
                pass

            def deaggregate(self) -> dict:
                pass

            @staticmethod
            def requires() -> List[Union[str, Type]]:
                return []

            def run(self):  # pragma: no cover
                pass

        with self.assertRaises(DependencyGraphGenerationError):
            BadAggregate(
                "record_id",
                ConfigManager.ROOT,
                ConfigManager(Path("dep-graph-config.yaml")),
                {},
                os.getcwd(),
                False
            )


if __name__ == '__main__':
    unittest.main()
