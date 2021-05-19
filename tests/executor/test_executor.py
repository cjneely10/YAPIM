import os
from pathlib import Path
from typing import Dict
from unittest import TestCase

from src.executor import Executor
from src.extension_loader import ExtensionLoader
from src.utils.input_loader import InputLoader


class TestExecutor(TestCase):
    file = Path(os.path.dirname(__file__)).resolve()

    def test_simple(self):

        class TestLoader(InputLoader):
            def __init__(self, n: int):
                self.n = n

            def load(self) -> Dict[str, Dict]:
                return {str(i): {} for i in range(self.n)}

        Executor(
            TestLoader(100),  # Input loader
            TestExecutor.file.joinpath("simple/sample-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("simple-out"),  # Base output dir path
            "simple/sample_tasks1",  # Relative path to pipeline directory
            ["simple/sample_dependencies"],  # List of relative paths to dependency directories,
            False  # Silence status messages
        ).run()

    def test_fasta(self):
        out_dir = TestExecutor.file.joinpath("fasta-out")
        Executor(
            ExtensionLoader(  # Input loader
                Path("/media/user/5FB965DD5569ACE6/Data/tmp").resolve(),
                out_dir.joinpath("MAGs"),
            ),
            TestExecutor.file.joinpath("fasta/fasta-config.yaml"),  # Config file path
            out_dir,  # Base output dir path
            "fasta/tasks",  # Relative path to pipeline directory
            display_status_messages=False
        ).run()

    def test_nested(self):
        out_dir = TestExecutor.file.joinpath("nested-out")
        Executor(
            ExtensionLoader(  # Input loader
                Path("/media/user/5FB965DD5569ACE6/Data/tmp").resolve(),
                out_dir.joinpath("MAGs"),
            ),
            TestExecutor.file.joinpath("nested/nested-config.yaml"),  # Config file path
            out_dir,  # Base output dir path
            "nested/tasks",  # Relative path to pipeline directory
        ).run()
