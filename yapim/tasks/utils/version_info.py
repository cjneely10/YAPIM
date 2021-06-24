from typing import Optional


class VersionInfo:
    def __init__(self, version: str, calling_parameter: str, program_name: Optional[str] = None):
        self._version = version
        self._calling_parameter = calling_parameter
        self._program = program_name

    @property
    def program(self) -> Optional[str]:
        return self._program

    @property
    def version(self) -> str:
        return self._version

    @property
    def calling_parameter(self) -> str:
        return self._calling_parameter
