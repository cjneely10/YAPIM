import unittest

from yapim.utils.config_manager import *
from yapim.utils.config_manager_generator import ConfigManagerGenerator


class TestConfigManager(unittest.TestCase):
    cfg = ConfigManager(Path("config_files/valid-config.yaml"))

    def test_valid(self):
        print(TestConfigManager.cfg)

    def test_bad_data(self):
        with self.assertRaises(MissingDataError):
            ConfigManager(Path("config_files/bad_data-config.yaml"))

    def test_invalid_program(self):
        with self.assertRaises(InvalidPathError):
            ConfigManager(Path("config_files/bad_program-config.yaml"))

    def test_missing_program(self):
        with self.assertRaises(InvalidPathError):
            ConfigManager(Path("config_files/missing_program-config.yaml"))

    def test_invalid_dependencies_section(self):
        with self.assertRaises(MissingDataError):
            ConfigManager(Path("config_files/invalid_dependency_section-config.yaml"))

    def test_slurm_info(self):
        self.assertEqual(
            [('--job-name', 'EukMS'), ('--qos', 'unlim')],
            TestConfigManager.cfg.get_sbatch_flagged_arguments()
        )

    def test_get_outer(self):
        self.assertEqual(
            TestConfigManager.cfg.config["Sample"],
            TestConfigManager.cfg.get((ConfigManager.ROOT, "Sample"))
        )

    def test_get_inner(self):
        self.assertEqual(
            TestConfigManager.cfg.config["Sample"][ConfigManager.DEPENDENCIES]["Value"],
            TestConfigManager.cfg.get(("Sample", "Value"))
        )

    def test_parent_info(self):
        self.assertEqual(
            TestConfigManager.cfg.config["Sample"],
            TestConfigManager.cfg.parent_info(("Sample", "Value")),
        )

    def test_find_outer(self):
        self.assertEqual(
            "sample-data.list",
            TestConfigManager.cfg.find((ConfigManager.ROOT, "Sample"), ConfigManager.DATA)
        )

    def test_find_inner(self):
        self.assertEqual(
            "cat",
            TestConfigManager.cfg.find(("Sample", "Value"), ConfigManager.PROGRAM)
        )

    def test_find_inner_overwrites(self):
        self.assertEqual(
            "sample-data2.list",
            TestConfigManager.cfg.find(("Sample", "Value"), ConfigManager.DATA)
        )

    def test_find_missing(self):
        self.assertEqual(
            None,
            TestConfigManager.cfg.find(("Sample", "Value"), "meow")
        )

    def test_file_generation(self):
        ConfigManagerGenerator("sample_tasks1", ["sample_dependencies"]).write(Path("output-config.yaml").resolve())
        original_fp = open("original-config.yaml", "r")
        new_fp = open("output-config.yaml", "r")
        self.assertEqual(
            original_fp.readlines(),
            new_fp.readlines(),
        )
        original_fp.close()
        new_fp.close()

    def test_bad_resources(self):
        with self.assertRaises(InvalidResourcesError):
            ConfigManager(Path("config_files/bad_resources-config.yaml"))


if __name__ == '__main__':
    unittest.main()
