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

    def all_priors(self):
        return list(self.collect_by.keys())
