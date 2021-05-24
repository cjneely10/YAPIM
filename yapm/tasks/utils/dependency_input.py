"""
Module holds DependencyInput `struct` that is used to in TaskList.depends member
"""

from typing import Dict, Optional, Union, Type


class DependencyInput:
    """ Class handles parsing input for dependency requirements

    """
    def __init__(self, name: Union[str, Type],
                 collect_by: Optional[Dict[str, Dict[str, str]]] = None):
        self.name = name
        if collect_by is None:
            self.collect_by = None
        else:
            self.collect_by = collect_by

    def __str__(self):  # pragma: no cover
        return f"<Dependency name: {self.name}, collection: {self.collect_by}>"

    def __repr__(self):  # pragma: no cover
        return self.__str__()