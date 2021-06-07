import os
from abc import ABC
from importlib import import_module
from inspect import isclass
from typing import Type

from yapim import InputLoader


class PackageManager(ABC):
    pipeline_file = ".pipeline.pkl"

    @staticmethod
    def _get_loader(loader_name: str) -> Type[InputLoader]:
        current_dir = os.getcwd()
        os.chdir(os.path.dirname(loader_name))
        module = import_module(os.path.basename(os.path.splitext(loader_name)[0]))
        for attribute_name in dir(module):
            attribute = getattr(module, attribute_name)
            if isclass(attribute) and issubclass(attribute, InputLoader) and loader_name in attribute.__name__:
                os.chdir(current_dir)
                return attribute
        os.chdir(current_dir)
        print("Unable to import loader module")
        exit(1)
