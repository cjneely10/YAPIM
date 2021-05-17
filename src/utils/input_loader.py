from abc import ABC, abstractmethod
from typing import Dict


class InputLoader(ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def load(self) -> Dict[str, Dict]:
        """

        :return:
        :rtype:
        """
