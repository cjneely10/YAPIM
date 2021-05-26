import os
import unittest
from pathlib import Path
from typing import Dict

from yapim.tasks.utils.base_task import BaseTask
from yapim.utils.executor import Executor
from yapim.utils.extension_loader import ExtensionLoader
from yapim.utils.input_loader import InputLoader


class TestExecutor(unittest.TestCase):
    file = Path(os.path.dirname(__file__)).resolve()

    class TestLoader(InputLoader):
        def __init__(self, n: int):
            self.n = n

        def load(self) -> Dict[str, Dict]:
            return {str(i): {} for i in range(self.n)}

    def test_simple(self):
        Executor(
            TestExecutor.TestLoader(10),  # Input loader
            TestExecutor.file.joinpath("simple/sample-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("simple-out"),  # Base output dir path
            Path("simple/sample_tasks1"),  # Relative path to pipeline directory
            [Path("simple/sample_dependencies")],  # List of relative paths to dependency directories,
            display_status_messages=False  # Silence status messages
        ).run()

    def test_fasta(self):
        out_dir = TestExecutor.file.joinpath("fasta-out")
        Executor(
            ExtensionLoader(  # Input loader
                Path("../data").resolve(),
                out_dir.joinpath("MAGs"),
            ),
            TestExecutor.file.joinpath("fasta/fasta-config.yaml"),  # Config file path
            out_dir,  # Base output dir path
            Path("fasta/tasks"),  # Relative path to pipeline directory
            display_status_messages=False
        ).run()

    def test_nested(self):
        out_dir = TestExecutor.file.joinpath("nested-out")
        Executor(
            ExtensionLoader(  # Input loader
                Path("../data").resolve(),
                out_dir.joinpath("MAGs"),
            ),
            TestExecutor.file.joinpath("nested/nested-config.yaml"),  # Config file path
            out_dir,  # Base output dir path
            Path("nested/tasks"),  # Relative path to pipeline directory
            [Path("nested/mmseqs_dependencies")],
        ).run()

    def test_no_output_defined(self):
        Executor(
            TestExecutor.TestLoader(10),  # Input loader
            TestExecutor.file.joinpath("no_output/no_output-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("no_output-out"),  # Base output dir path
            "no_output/tasks",  # Relative path to pipeline directory
        ).run()

    def test_missing_output(self):
        with self.assertRaises(BaseTask.TaskCompletionError):
            Executor(
                TestExecutor.TestLoader(10),  # Input loader
                TestExecutor.file.joinpath("missing_output/missing_output-config.yaml"),  # Config file path
                TestExecutor.file.joinpath("missing_output-out"),  # Base output dir path
                "missing_output",  # Relative path to pipeline directory
            ).run()

    def test_bad_program_path(self):
        Executor(
            TestExecutor.TestLoader(1),  # Input loader
            TestExecutor.file.joinpath("bad_program_path/bad_program_path-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("bad_program_path-out"),  # Base output dir path
            "bad_program_path",  # Relative path to pipeline directory
            display_status_messages=False
        ).run()

    def test_existing_data(self):
        Executor(
            TestExecutor.TestLoader(1),  # Input loader
            TestExecutor.file.joinpath("existing_data/first_pipeline-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("existing_data-out"),  # Base output dir path
            "existing_data/first_pipeline",  # Relative path to pipeline directory
            ["existing_data/sample_dependencies"]
        ).run()

        Executor(
            TestExecutor.TestLoader(1),  # Input loader
            TestExecutor.file.joinpath("existing_data/second_pipeline-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("existing_data-out"),  # Base output dir path
            "existing_data/second_pipeline",  # Relative path to pipeline directory
        ).run()

    def test_aggregate_dependencies(self):
        Executor(
            TestExecutor.TestLoader(1),  # Input loader
            TestExecutor.file.joinpath("aggregate_dependency/aggregate-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("aggregate_dependency-out"),  # Base output dir path
            "aggregate_dependency/tasks",  # Relative path to pipeline directory
            ["aggregate_dependency/dependencies"]
        ).run()


if __name__ == '__main__':
    unittest.main()
