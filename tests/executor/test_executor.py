import os
from pathlib import Path
from typing import Dict
from unittest import TestCase

from HPCBioPipe.tasks.utils.base_task import BaseTask
from HPCBioPipe.utils.executor import Executor
from HPCBioPipe.utils.extension_loader import ExtensionLoader
from HPCBioPipe.utils.input_loader import InputLoader


class TestExecutor(TestCase):
    file = Path(os.path.dirname(__file__)).resolve()

    class TestLoader(InputLoader):
        def __init__(self, n: int):
            self.n = n

        def load(self) -> Dict[str, Dict]:
            return {str(i): {} for i in range(self.n)}

    def test_simple(self):

        Executor(
            TestExecutor.TestLoader(50),  # Input loader
            TestExecutor.file.joinpath("simple/sample-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("simple-out"),  # Base output dir path
            "simple/sample_tasks1",  # Relative path to pipeline directory
            ["simple/sample_dependencies"],  # List of relative paths to dependency directories,
            display_status_messages=False  # Silence status messages
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
            ["nested/mmseqs_dependencies"],
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

    def test_bad_program_path_config(self):
        self.fail()

    def test_bad_program_path_from_within_class_definition(self):
        self.fail()
