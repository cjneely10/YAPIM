import os
from pathlib import Path
from unittest import TestCase

from src.executor import Executor


class TestExecutor(TestCase):
    sample_tasks1_dir = Path(os.path.dirname(__file__)).resolve().joinpath("sample_tasks1")
    sample_dependencies_dir = Path(os.path.dirname(__file__)).resolve().joinpath("sample_dependencies")

    def test_run(self):
        executor = Executor(
            TestExecutor.sample_tasks1_dir.joinpath("sample-config.yaml"),
            TestExecutor.sample_tasks1_dir,
            Path(os.path.join(os.path.dirname(__file__), "out")).resolve(),
            {"one": {}, "two": {}},
            TestExecutor.sample_dependencies_dir
        )
        executor.run()
