"""Top-level ABC for loading input into pipeline. Developers may extend InputLoader to create customized pipeline
input loaders"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict

from yapim.utils.existing_input_loader import ExistingInputLoader


class InputLoader(ABC):
    """Load input into pipeline with provided abstract methods. Executor will use InputLoader subclass instance
    to populate input prior to launching pipeline"""
    @abstractmethod
    def load(self) -> Dict[str, Dict]:
        """ Populate input into dictionary of {record_id: {}} and return

        :return:
        :rtype:
        """

    @abstractmethod
    def storage_directory(self) -> Path:
        """ Location used for storing populated input

        :return:
        :rtype:
        """

    @staticmethod
    def populate_requested_existing_input(input_section: dict, results_base_dir: str) -> Dict[str, Dict]:
        """Load existing pipeline data referenced in configuration file"""
        return ExistingInputLoader(input_section, results_base_dir).populate()
