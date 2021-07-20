"""Wrap Task/AggregateTask as dependency for separate Task. Make your code reusable!"""

from typing import Dict, Optional, Union, Type, MutableSequence


class DependencyInput:
    """Class handles parsing input for dependency requirements"""
    def __init__(self, name: Union[str, Type],
                 collect_by: Optional[Dict[str, Union[Dict[str, str], MutableSequence[str]]]] = None):
        """ `Task` or `AggregateTask` that acts to complete some intermediary step to complete a different
        `Task`/`AggregateTask` (hence, a dependency)

        def requires() -> List[Union[str, Type]]:
            return ["A", "B"]


        def depends() -> List[DependencyInput]:
            return [
                DependencyInput("DependencyName", {"A": {"from": "to"}, "B": ["from"]})
            ]
        """
        self.name = name
        self.collect_by = collect_by

    def __str__(self):  # pragma: no cover
        return f"<Dependency name: {self.name}, collection: {self.collect_by}>"

    def __repr__(self):  # pragma: no cover
        return self.__str__()
