import os
from pathlib import Path
from typing import List

from yapim import AggregateTask, DependencyInput


class Merge(AggregateTask):
    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "file": self.aggregate(),
            "final": ["file"]
        }

    @staticmethod
    def requires() -> List[str]:
        return ["Write"]

    def run(self):
        fp = open(self.input["file"], "r")
        print(fp.readlines())
        fp.close()

    def aggregate(self):
        file_path = os.path.join(self.wdir, "aggregate-file.txt")
        paths_file = open(file_path, "w")
        for record_id in self.input.keys():
            paths_file.write("\t".join([str(record_id), str(self.input[record_id]["Write"]["result"])]))
            paths_file.write("\n")
        paths_file.close()
        return Path(file_path).resolve()

    def deaggregate(self) -> dict:
        fp = open(self.output["file"], "r")
        out = {}
        for i, line in enumerate(fp):
            if i % 2 == 0:
                continue
            line = line.rstrip("\r\n").split("\t")
            out[line[0]] = line[1]
        fp.close()
        return out
