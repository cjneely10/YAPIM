import pickle
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict


class InputLoader(ABC):
    @abstractmethod
    def load(self) -> Dict[str, Dict]:
        """ Populate input into dictionary of {record_id: {}} and return

        :return:
        :rtype:
        """

    @abstractmethod
    def storage_directory(self):
        """

        :return:
        :rtype:
        """

    @staticmethod
    def load_pkl_data(pkl_file: Path) -> Dict[str, Dict]:
        """ Load a pkl result file into a dictionary. Useful for when populating input from completed pipelines

        :return:
        """
        if pkl_file.exists():
            with open(pkl_file, "rb") as fp:
                return pickle.load(fp)
        return {}
