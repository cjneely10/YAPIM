import os
from pathlib import Path
from typing import Dict
from unittest import TestCase

from src.executor import Executor
from src.utils.input_loader import InputLoader


class TestExecutor(TestCase):

    def test_simple(self):
        sample_tasks1_dir = Path(os.path.dirname(__file__)).resolve().joinpath("sample_tasks1")
        sample_dependencies_dir = Path(os.path.dirname(__file__)).resolve().joinpath("sample_dependencies")
        sample_config_file = Path(os.path.dirname(__file__)).resolve().joinpath("sample-config.yaml")

        class TestLoader(InputLoader):
            def __init__(self, n: int):
                self.n = n

            def load(self) -> Dict[str, Dict]:
                return {str(i): {} for i in range(self.n)}

        executor = Executor(
            TestLoader(10),
            sample_config_file,
            Path(os.path.join(os.path.dirname(__file__), "out")).resolve(),
            sample_tasks1_dir,
            [sample_dependencies_dir]
        )
        executor.run()

    def test_fasta_loader(self):
        self.fail()
