import pkgutil
from abc import ABC
from inspect import isclass, isabstract
from pathlib import Path
from typing import Type

from yapim.utils.input_loader import InputLoader


class PackageManager(ABC):
    pipeline_file = ".pipeline.pkl"

    @staticmethod
    def _get_loader(pipeline_dir: Path) -> Type[InputLoader]:
        for loader, module_name, _ in pkgutil.walk_packages([str(pipeline_dir)]):
            module = loader.find_module(module_name).load_module(module_name)
            for attribute_name in dir(module):
                attribute = getattr(module, attribute_name)
                if isclass(attribute) and issubclass(attribute, InputLoader) and not isabstract(attribute):
                    # Add the class to this package's variables
                    return attribute
        print("Unable to import loader module")
        exit(1)
