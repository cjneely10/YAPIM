"""
Populate dependencies for easy loading
"""
import pkgutil
from inspect import isclass, isabstract
from pathlib import Path

from yapim.tasks.task import Task


def get_modules(package_dir: Path) -> dict:
    out = {}
    for loader, module_name, is_pkg in pkgutil.walk_packages([str(package_dir)]):
        module = loader.find_module(module_name).load_module(module_name)
        for attribute_name in dir(module):
            attribute = getattr(module, attribute_name)
            if isclass(attribute) and issubclass(attribute, Task) and not isabstract(attribute):
                # Add the class to this package's variables
                out[attribute.__name__] = attribute
    return out
