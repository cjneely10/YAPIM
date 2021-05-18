import os
from pathlib import Path
from typing import Dict
from unittest import TestCase

from src.executor import Executor
from src.fasta_loader import FastaLoader
from src.utils.input_loader import InputLoader


class TestExecutor(TestCase):

    def test_simple(self):
        sample_config_file = Path(os.path.dirname(__file__)).resolve().joinpath("simple/sample-config.yaml")

        class TestLoader(InputLoader):
            def __init__(self, n: int):
                self.n = n

            def load(self) -> Dict[str, Dict]:
                return {str(i): {} for i in range(self.n)}

        executor = Executor(
            TestLoader(10),  # Input loader
            sample_config_file,  # Config file path
            Path(os.path.join(os.path.dirname(__file__), "out")).resolve(),  # Base output dir path
            "simple/sample_tasks1",  # Relative path to pipeline directory
            ["simple/sample_dependencies"]  # List of relative paths to dependency directories
        )
        executor.run()

    def test_fasta_loader(self):
        out_dir = Path(os.path.join(os.path.dirname(__file__), "fasta-out")).resolve()
        sample_config_file = Path(os.path.dirname(__file__)).resolve().joinpath("fasta/fasta-config.yaml")
        executor = Executor(
            FastaLoader(
                Path("/media/user/5FB965DD5569ACE6/Data/EukMS-Test/data/genomes").resolve(),
                out_dir.joinpath("MAGs"),
                {".fna"}
            ),  # Input loader
            sample_config_file,  # Config file path
            out_dir,  # Base output dir path
            "fasta/tasks",  # Relative path to pipeline directory
        )
        executor.run()
