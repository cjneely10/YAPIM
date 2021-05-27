"""
Module holds DependencyInput `struct` that is used to in TaskList.depends member
"""

from typing import Dict, Optional, Union, Type, MutableSequence


class DependencyInput:
    """ Class handles parsing input for dependency requirements

    """
    def __init__(self, name: Union[str, Type],
                 collect_by: Optional[Dict[str, Union[Dict[str, str], MutableSequence[str]]]] = None):
        self.name = name
        self.collect_by = collect_by

    def __str__(self):  # pragma: no cover
        return f"<Dependency name: {self.name}, collection: {self.collect_by}>"

    def __repr__(self):  # pragma: no cover
        return self.__str__()
