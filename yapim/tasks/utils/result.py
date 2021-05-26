class Result:
    def __init__(self, value):
        self._value = value

    def __get__(self):  # pragma: no cover
        return str(self._value)

    def __str__(self):  # pragma: no cover
        return str(self._value)
