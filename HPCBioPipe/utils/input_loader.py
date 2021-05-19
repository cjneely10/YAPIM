from abc import ABC, abstractmethod
from typing import Dict


class InputLoader(ABC):
    @abstractmethod
    def load(self) -> Dict[str, Dict]:
        """

        :return:
        :rtype:
        """
