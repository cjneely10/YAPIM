"""
General functions

"""

import os
from pathlib import Path
from typing import Union


def touch(_path: Union[str, Path]):
    """ Mimic linux "touch" command

    :param _path: Empty file path to create, or to check if it exists
    """
    open(_path, "a").close()


def prefix(_path: Union[str, Path]) -> str:
    """ Extract prefix from file path

    :param _path: Path-like string
    :return: For path /path/to/file.txt return file
    """
    return os.path.basename(os.path.splitext(_path)[0])
