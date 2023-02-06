"""Utilities to manage removal of temporarily-created data"""
import os
import shutil
from glob import glob
from inspect import currentframe
from pathlib import Path
from typing import Union, Callable


# pylint: disable=too-few-public-methods
class _DeferredFString:
    """
    Defer execution of f-string to allow for API-provided + unevaluated templates

    See:
    https://stackoverflow.com/questions/42497625/how-to-postpone-defer-the-evaluation-of-f-strings
    """
    def __init__(self, payload: Union[Path, str]):
        self.payload = str(payload)

    def __str__(self):
        _vars = currentframe().f_back.f_globals.copy()
        _vars.update(currentframe().f_back.f_locals)
        return self.payload.format(**_vars)


def clean(*paths: Union[Path, str]):
    """
    Remove files/directories in this Task's working directory after run() completes

    Examples:
        @clean("tmp", "a*.out", "{self.record_id}.tmp.out", Path("dd").joinpath("tmp.out"))

    Note that strings will be interpolated as deferred f-strings

    Prior to performing delete operation, each path is concatenated with the record's working directory:
        glob.glob(self.wdir.joinpath(path))

    :param paths: Relative paths to remove
    :return:
    """

    def method(func: Callable):
        def fxn(self):
            func(self)
            for templated_path in paths:
                glob_path = _DeferredFString(templated_path)
                for out_fd in glob(str(self.wdir.joinpath(str(glob_path)))):
                    out_fd = Path(out_fd).resolve()
                    if out_fd.exists():
                        if out_fd.is_dir():
                            shutil.rmtree(out_fd, ignore_errors=True)
                        else:
                            os.remove(out_fd)

        return fxn

    return method
