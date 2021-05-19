"""
Populate dependencies for easy loading
"""

from importlib import import_module
from inspect import isclass
from pkgutil import iter_modules

from src.tasks.task import Task


def get_modules(package_dir: str) -> dict:
    """ Dynamically load all modules contained at sublevel

    :param package_dir: Directory from location where this function is called
    :return: Dict of task.name: Task object class
    """
    # iterate through the modules in the current package
    out = {}
    for (_, module_name, _) in iter_modules([package_dir]):
        # import the module and iterate through its attributes
        module = import_module(
            "{}.{}".format(
                package_dir.replace("/", "."),
                module_name
            )
        )
        for attribute_name in dir(module):
            attribute = getattr(module, attribute_name)
            if isclass(attribute) and issubclass(attribute, Task) and "Task" not in attribute.__name__:
                # Add the class to this package's variables
                out[attribute.__name__] = attribute
    return out
