"""Track version info for calling programs, allow/disallow versions and provide logic to run"""


from typing import Optional


class VersionInfo:
    """Struct to track allowed versions of a given program"""
    def __init__(self, version: str, calling_parameter: str, config_file_param_name: Optional[str] = None):
        """

        :param version: Version string output by program
        :param calling_parameter: Parameter to pass to program to output version info
        :param config_file_param_name: Name assigned to program in config file section (e.g., `program`, etc.)
        """
        self._version = version
        self._calling_parameter = calling_parameter
        self._config_file_param = config_file_param_name

    @property
    def config_param(self) -> Optional[str]:
        """Parameter name in config file"""
        return self._config_file_param

    @property
    def version(self) -> str:
        """Version"""
        return self._version

    @property
    def calling_parameter(self) -> str:
        """Parameter used to gather version info at the command line"""
        return self._calling_parameter
