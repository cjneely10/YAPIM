#!/usr/bin/env python

"""
Module holds logic to generate TaskList/Task class stub and associated config file sections
"""

import os

from plumbum import cli

# 0 - Class name
# 1 - Parent class
# 2 - 0.lower()
# 3 - If Aggregate task, create blank method, otherwise fill with empty string
BOILERPLATE = '''from HPCBioPipe import {1}, set_complete


class {0}({1}):
    task_name = "{2}"
    requires = []
    depends = []

    @set_complete
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            
        }
    {3}
    def run(self):
        """
        Run!
        """

'''


class ClassCreationRunner(cli.Application):
    step_name: str
    creation_directory: str = os.getcwd()
    base_class: str = "Task"

    def main(self, *args):
        pass

    @staticmethod
    def create_class_file(file_path: str, class_name: str, cfg_name: str):
        """ Create class file at provided path. Will create given class using name and will
        register class as using cfg_name within config file

        :param file_path: Output path for class stub
        :param class_name: Name to give class
        :param cfg_name: Config section name
        """
        file_ptr = open(os.path.join(file_path, cfg_name.replace(".", "_") + ".py"), "w")
        file_ptr.write(BOILERPLATE.format(class_name, cfg_name, """{
                    
                }"""))
        file_ptr.close()


if __name__ == "__main__":
    ClassCreationRunner.run()
