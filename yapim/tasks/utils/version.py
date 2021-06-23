class Version:
    def __init__(self, version_string: str, calling_parameter: str):
        self._version_string = version_string
        self._calling_parameter = calling_parameter

    @property
    def version(self):
        return self._version_string

    @property
    def calling_parameter(self):
        return self._calling_parameter
