import glob
import os
import unittest
from pathlib import Path
from typing import Dict, List

from yapim import Executor, InputLoader, ExtensionLoader
from yapim.utils.config_manager import ConfigManager
from yapim.utils.package_management.directory_cleaner import DirectoryCleaner
from yapim.utils.path_manager import PathManager


class TestCLI(unittest.TestCase):
    file = Path(os.path.dirname(__file__)).resolve()

    class TestLoader(InputLoader):
        def __init__(self, n: int):
            self.n = n

        def load(self) -> Dict[str, Dict]:
            return {str(i): {} for i in range(self.n)}

    @staticmethod
    def confirm_deleted_ids(output_directory: Path, deleted_ids: List[str]):
        deleted_ids = set(deleted_ids)
        wdir_location = output_directory.joinpath(PathManager.WDIR)
        for record_id in os.listdir(wdir_location):
            if record_id in deleted_ids:
                raise ValueError(f"Deleted id {record_id} is still present!")
        results_location = output_directory.joinpath(PathManager.RESULTS)
        for record_id in os.listdir(results_location):
            if record_id in deleted_ids:
                raise ValueError(f"Deleted id {record_id} is still present!")
        storage_location = output_directory.joinpath(ConfigManager.STORAGE_DIR)
        for record_id in deleted_ids:
            if len(glob.glob(str(storage_location.joinpath(record_id)) + "*")) > 0:
                raise ValueError(f"Deleted id {record_id} is still present!")

    @staticmethod
    def confirm_deleted_steps(output_directory: Path, expected_deleted: List[str]):
        expected_deleted = set(expected_deleted)
        wdir_location = output_directory.joinpath(PathManager.WDIR)
        for record_id in os.listdir(wdir_location):
            record_wdir = wdir_location.joinpath(record_id)
            for task_name in os.listdir(record_wdir):
                if task_name in expected_deleted:
                    raise ValueError(f"Deleted task {task_name} is still present for {record_id}!")

    def test_remove_ids(self):
        # Generate output
        out_dir = TestCLI.file.joinpath("fasta-out")
        Executor(
            ExtensionLoader(  # Input loader
                Path("../data").resolve(),
                out_dir,
            ),
            TestCLI.file.joinpath("fasta/fasta-config.yaml"),  # Config file path
            out_dir,  # Base output dir path
            Path("fasta/tasks"),  # Relative path to pipeline directory
            display_status_messages=False
        ).run()
        # Delete selected ids
        ids_to_delete = ["GCA_000002975.1_ASM297v1_genomic"]
        DirectoryCleaner(out_dir).remove(ids_to_delete)
        TestCLI.confirm_deleted_ids(out_dir, ids_to_delete)

    def test_delete_steps(self):
        # Generate output
        out_dir = TestCLI.file.joinpath("fasta-out")
        top_pipeline_dir = TestCLI.file.joinpath("fasta-pipeline")
        Executor(
            ExtensionLoader(  # Input loader
                Path("../data").resolve(),
                out_dir,
            ),
            top_pipeline_dir.joinpath("fasta-config.yaml"),  # Config file path
            out_dir,  # Base output dir path
            top_pipeline_dir.joinpath("fasta"),  # Relative path to pipeline directory
            display_status_messages=False
        ).run()
        # Delete selected ids
        ids_to_delete = ["Align"]
        DirectoryCleaner(out_dir).clean(top_pipeline_dir, ["Align"])
        TestCLI.confirm_deleted_steps(out_dir, ids_to_delete)


if __name__ == '__main__':
    unittest.main()
