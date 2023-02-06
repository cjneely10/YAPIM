import os
from pathlib import Path
from typing import List, Union, Type

from yapim import Task, DependencyInput, touch, clean


class TaskWithCleanup(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "retained_files": self.wdir.joinpath(f"{self.record_id}.txt")
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    @clean("wdir",
           "*tmp.out",
           "{self.record_id}.deferred.out",
           Path("keep").joinpath("file.out"),
           Path("keep").joinpath("{self.record_id}.deferred.out"))
    def run(self):
        touch(self.output["retained_files"])
        touch(self.wdir.joinpath(f"{self.record_id}tmp.out"))
        touch(self.wdir.joinpath(f"{self.record_id}.deferred.out"))
        os.mkdir(self.wdir.joinpath("wdir"))
        keep_dir = self.wdir.joinpath("keep")
        os.mkdir(keep_dir)
        touch(keep_dir.joinpath("file.out"))
        touch(keep_dir.joinpath(f"{self.record_id}.deferred.out"))
