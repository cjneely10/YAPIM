from typing import Optional


class VersionInfo:
    def __init__(self, version: str, calling_parameter: str, config_file_param_name: Optional[str] = None):
        self._version = version
        self._calling_parameter = calling_parameter
        self._config_file_param = config_file_param_name

    @property
    def config_param(self) -> Optional[str]:
        return self._config_file_param

    @property
    def version(self) -> str:
        return self._version

    @property
    def calling_parameter(self) -> str:
        return self._calling_parameter
