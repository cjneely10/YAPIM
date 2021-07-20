"""Wrapper class for hanlding strin"""


class Result:
    """
    Wrapper class around str/Path types to prevent Task from marking a str/Path that doesn't exist as an execution error
    """
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        """Get object stored in Result"""
        return self._value

    def __get__(self, *args):  # pragma: no cover
        return str(self._value)

    def __str__(self):  # pragma: no cover
        return str(self._value)

    def __repr__(self):  # pragma: no cover
        return str(self)
