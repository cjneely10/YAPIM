from abc import ABC, abstractmethod
from typing import Dict


# TODO: Handle loading completed results from .pkl file
class InputLoader(ABC):
    @abstractmethod
    def load(self) -> Dict[str, Dict]:
        """

        :return:
        :rtype:
        """
