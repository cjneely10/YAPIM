"""Top-level ABC for loading input into pipeline. Developers may extend InputLoader to create customized pipeline
input loaders"""

import pickle
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict


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
    def load_pkl_data(pkl_file: Path) -> Dict[str, Dict]:
        """ Load a pkl result file into a dictionary. Useful for when populating input from completed pipelines

        :return:
        """
        with open(pkl_file, "rb") as file_ptr:
            return pickle.load(file_ptr)
