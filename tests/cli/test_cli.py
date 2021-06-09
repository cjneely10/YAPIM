import os
import glob
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


if __name__ == '__main__':
    unittest.main()
