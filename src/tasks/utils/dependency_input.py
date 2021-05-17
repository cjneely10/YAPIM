"""
Module holds DependencyInput `struct` that is used to in TaskList.depends member
"""

from typing import Dict, Optional, Iterable


# pylint: disable=too-few-public-methods
class DependencyInput:
    """ Class handles parsing input for dependency requirements

    """
    class DependencyCreationError(TypeError):
        def __init__(self):
            super().__init__("Task creation error - must provide a task name XOR a mapping of {task_name: {from: to}}")

    def __init__(self, name: str,
                 collect_all: Optional[Iterable[str]],
                 collect_by: Optional[Iterable[Dict[str, Dict[str, str]]]]):
        self.name = name
        self.collect_all = collect_all
        self.collect_by = collect_by

    def all_priors(self):
        out = []
        if self.collect_all is not None:
            out += list(self.collect_all)
        if self.collect_by is not None:
            for collection in self.collect_by:
                out += list(collection.keys())
        return out
