"""
Populate dependencies for easy loading
"""

import os
from pathlib import Path
from inspect import isclass
from pkgutil import iter_modules
from importlib import import_module

from src.tasks.task import Task


def get_modules(package_dir: Path) -> dict:
    """ Dynamically load all modules contained at sublevel

    :param package_dir: Directory from location where this function is called
    :return: Dict of task.name: Task object class
    """
    # iterate through the modules in the current package
    package_dir = Path(package_dir).resolve().parent
    out = {}
    for (_, module_name, _) in iter_modules([package_dir]):
        # import the module and iterate through its attributes
        module = import_module(
            "{}.{}".format(
                os.path.splitext(os.path.basename(package_dir))[0],
                module_name
            )
        )
        for attribute_name in dir(module):
            attribute = getattr(module, attribute_name)
            if isclass(attribute) and issubclass(attribute, Task):
                # Add the class to this package's variables
                out[attribute.task_name] = attribute
    return out
