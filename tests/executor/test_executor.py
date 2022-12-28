import os
import shutil
import unittest
from pathlib import Path
from typing import Dict

from plumbum import CommandNotFound

from yapim import TaskExecutionError
from yapim.tasks.utils.base_task import BaseTask
from yapim.utils.dependency_graph import DependencyGraphGenerationError
from yapim.utils.executor import Executor
from yapim.utils.extension_loader import ExtensionLoader
from yapim.utils.input_loader import InputLoader


class ComplexInputType:
    def __init__(self, i: int):
        self.data = []
        for _ in range(i):
            self.data.append(1)

    def __str__(self):
        return f"{sum(self.data)}"


class ImproperComplexInputType:
    def __init__(self, i: int):
        self.data = []
        for _ in range(i):
            self.data.append(1)


class TestExecutor(unittest.TestCase):
    file = Path(os.path.dirname(__file__)).resolve()

    class Loader(InputLoader):
        def __init__(self, creation_type: type, n: int):
            self.n = n
            self.type = creation_type

        def load(self) -> Dict[str, Dict]:
            return {str(self.type(i)): {} for i in range(self.n)}

        def storage_directory(self):
            return ""

    class LoaderWithData(InputLoader):
        def __init__(self, n: int):
            self.n = n

        def load(self) -> Dict[str, Dict]:
            return {str(i): {"value": chr(i + ord('A'))} for i in range(self.n)}

        def storage_directory(self):
            return ""

    class SimpleLoader(Loader):
        def __init__(self, n: int):
            super().__init__(str, n)

    class ComplexLoader(Loader):
        def __init__(self, n: int):
            super().__init__(ComplexInputType, n)

    class ImproperComplexLoader(Loader):
        def __init__(self, n: int):
            super().__init__(ImproperComplexInputType, n)

    def test_simple(self):
        Executor(
            TestExecutor.SimpleLoader(10),  # Input loader
            TestExecutor.file.joinpath("simple").joinpath("sample-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("simple-out"),  # Base output dir path
            Path("simple").joinpath("sample_tasks1"),  # Relative path to pipeline directory
            [Path("simple").joinpath("sample_dependencies")],  # List of relative paths to dependency directories,
            display_status_messages=False  # Silence status messages
        ).run()

    def test_complex(self):
        out_dir = TestExecutor.file.joinpath("simple-out")
        if out_dir.exists():
            shutil.rmtree(out_dir)
        Executor(
            TestExecutor.ComplexLoader(10),  # Input loader
            TestExecutor.file.joinpath("simple").joinpath("sample-config.yaml"),  # Config file path
            out_dir,  # Base output dir path
            Path("simple").joinpath("sample_tasks1"),  # Relative path to pipeline directory
            [Path("simple").joinpath("sample_dependencies")],  # List of relative paths to dependency directories,
            # display_status_messages=False  # Silence status messages
        ).run()

    def test_improper_complex_type(self):
        with self.assertRaises(AttributeError):
            Executor(
                TestExecutor.ImproperComplexLoader(10),  # Input loader
                TestExecutor.file.joinpath("simple").joinpath("sample-config.yaml"),  # Config file path
                TestExecutor.file.joinpath("simple-out"),  # Base output dir path
                Path("simple").joinpath("sample_tasks1"),  # Relative path to pipeline directory
                [Path("simple").joinpath("sample_dependencies")],  # List of relative paths to dependency directories,
                # display_status_messages=False  # Silence status messages
            ).run()

    def test_fasta(self):
        out_dir = TestExecutor.file.joinpath("fasta-out")
        Executor(
            ExtensionLoader(  # Input loader
                Path("../data").resolve(),
                out_dir,
            ),
            TestExecutor.file.joinpath("fasta").joinpath("fasta-config.yaml"),  # Config file path
            out_dir,  # Base output dir path
            Path("fasta/tasks"),  # Relative path to pipeline directory
            display_status_messages=False
        ).run()

    def test_nested(self):
        out_dir = TestExecutor.file.joinpath("nested-out")
        Executor(
            ExtensionLoader(  # Input loader
                Path("../data").resolve(),
                out_dir,
            ),
            TestExecutor.file.joinpath("nested").joinpath("nested-config.yaml"),  # Config file path
            out_dir,  # Base output dir path
            Path("nested/tasks"),  # Relative path to pipeline directory
            [Path("nested/mmseqs_dependencies")],
        ).run()

    def test_no_output_defined(self):
        Executor(
            TestExecutor.SimpleLoader(10),  # Input loader
            TestExecutor.file.joinpath("no_output").joinpath("no_output-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("no_output-out"),  # Base output dir path
            "no_output/tasks",  # Relative path to pipeline directory
        ).run()

    def test_missing_output(self):
        with self.assertRaises(BaseTask.TaskCompletionError):
            Executor(
                TestExecutor.SimpleLoader(10),  # Input loader
                TestExecutor.file.joinpath("missing_output").joinpath("missing_output-config.yaml"),  # Config file path
                TestExecutor.file.joinpath("missing_output-out"),  # Base output dir path
                "missing_output",  # Relative path to pipeline directory
            ).run()

    def test_bad_program_path(self):
        with self.assertRaises(CommandNotFound):
            Executor(
                TestExecutor.SimpleLoader(1),  # Input loader
                TestExecutor.file.joinpath("bad_program_path").joinpath("bad_program_path-config.yaml"),  # Config file path
                TestExecutor.file.joinpath("bad_program_path-out"),  # Base output dir path
                "bad_program_path",  # Relative path to pipeline directory
                display_status_messages=False
            ).run()

    def test_existing_data(self):
        Executor(
            TestExecutor.LoaderWithData(10),  # Input loader
            TestExecutor.file.joinpath("existing_data").joinpath("first_pipeline-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("existing_data-out"),  # Base output dir path
            "existing_data/first_pipeline",  # Relative path to pipeline directory
            ["existing_data/sample_dependencies"]
        ).run()

        Executor(
            TestExecutor.LoaderWithData(10),  # Input loader
            TestExecutor.file.joinpath("existing_data").joinpath("second_pipeline-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("existing_data-out"),  # Base output dir path
            "existing_data/second_pipeline",  # Relative path to pipeline directory
        ).run()

        Executor(
            TestExecutor.LoaderWithData(10),  # Input loader
            TestExecutor.file.joinpath("existing_data").joinpath("third_pipeline-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("existing_data-out"),  # Base output dir path
            "existing_data/third_pipeline",  # Relative path to pipeline directory
        ).run()

    def test_aggregate_dependencies(self):
        Executor(
            TestExecutor.SimpleLoader(1),  # Input loader
            TestExecutor.file.joinpath("aggregate_dependency").joinpath("aggregate-config.yaml"),  # Config file path
            TestExecutor.file.joinpath("aggregate_dependency-out"),  # Base output dir path
            "aggregate_dependency/tasks",  # Relative path to pipeline directory
            ["aggregate_dependency/dependencies"]
        ).run()

    def test_nested_requirements(self):
        Executor(
            TestExecutor.SimpleLoader(10),
            TestExecutor.file.joinpath("nested_requirements").joinpath("nested_requirements-config.yaml"),
            TestExecutor.file.joinpath("nested_requirements-out"),
            "nested_requirements/tasks",
            ["nested_requirements/dependencies"]
        ).run()

    def test_bad_mixing(self):
        with self.assertRaises(DependencyGraphGenerationError):
            Executor(
                TestExecutor.SimpleLoader(10),
                TestExecutor.file.joinpath("bad_mixing").joinpath("bad_mixing-config.yaml"),
                TestExecutor.file.joinpath("bad_mixing-out"),
                "bad_mixing/tasks",
                ["bad_mixing/dependencies"]
            ).run()

    def test_cross_types_reqs_deps(self):
        Executor(
            TestExecutor.SimpleLoader(10),
            TestExecutor.file.joinpath("cross_types_reqs_deps").joinpath("cross_types_reqs_deps.yaml"),
            TestExecutor.file.joinpath("cross_types_reqs_deps-out"),
            "cross_types_reqs_deps/tasks",
            ["cross_types_reqs_deps/dependencies"]
        ).run()

    def test_added_imports(self):
        Executor(
            TestExecutor.SimpleLoader(10),
            TestExecutor.file.joinpath("added_imports").joinpath("added_imports-config.yaml"),
            TestExecutor.file.joinpath("added_imports-out"),
            "added_imports/tasks",
        ).run()

    def test_confirm_not_overwritten(self):
        class TestIDLoader(InputLoader):
            def __init__(self, n: int):
                self.n = n

            def storage_directory(self):
                return ""

            def load(self) -> Dict[str, Dict]:
                return {str(i): {"input": str(i), "other": "other"} for i in range(self.n)}

        Executor(
            TestIDLoader(10),
            TestExecutor.file.joinpath("confirm_not_overwritten").joinpath("confirm_not_overwritten-config.yaml"),
            TestExecutor.file.joinpath("confirm_not_overwritten-out"),
            "confirm_not_overwritten/tasks",
            ["confirm_not_overwritten/dependencies"]
        ).run()

    def test_improper_versioned_data(self):
        with self.assertRaises(TaskExecutionError):
            Executor(
                TestExecutor.SimpleLoader(10),
                TestExecutor.file.joinpath("improper_versioned_data").joinpath("improper_versioned_data-config.yaml"),
                TestExecutor.file.joinpath("improper_versioned_data-out"),
                "improper_versioned_data/tasks",
            ).run()

    def test_conditional_run(self):
        Executor(
            TestExecutor.SimpleLoader(10),
            TestExecutor.file.joinpath("conditional_task").joinpath("tasks-config.yaml"),
            TestExecutor.file.joinpath("conditional_task-out"),
            "conditional_task/tasks",
            ["conditional_task/dependencies"]
        ).run()


if __name__ == '__main__':
    unittest.main()
