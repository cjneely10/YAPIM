"""
Module holds DependencyInput `struct` that is used to in TaskList.depends member
"""

from typing import Dict, Optional

# pylint: disable=too-few-public-methods
from src.utils.config_manager import ConfigManager


class DependencyInput:
    """ Class handles parsing input for dependency requirements

    """
    def __init__(self, name: str,
                 collect_by: Optional[Dict[str, Dict[str, str]]] = None):
        self.name = name
        if collect_by is None:
            self.collect_by = {ConfigManager.ROOT: {}}
        else:
            self.collect_by = collect_by

    def __str__(self):  # pragma: no cover
        return f"<Dependency name: {self.name}, collection: {self.collect_by}>"

    def __repr__(self):  # pragma: no cover
        return self.__str__()
