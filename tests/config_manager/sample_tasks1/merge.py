import os
from pathlib import Path
from typing import List

from HPCBioPipe import AggregateTask, DependencyInput


class Merge(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "file": self.input["file"],
            "final": ["file"]
        }

    @staticmethod
    def requires() -> List[str]:
        return ["Write"]

    def run(self):
        fp = open(self.input["file"], "r")
        print(fp.readlines())
        fp.close()

    def aggregate(self) -> dict:
        file_path = os.path.join(self.wdir, "aggregate-file.txt")
        paths_file = open(file_path, "w")
        for record_id in self.input.keys():
            paths_file.write("\t".join([record_id, str(self.input[record_id]["Write"]["result"])]))
            paths_file.write("\n")
        paths_file.close()
        return {"file": Path(file_path).resolve()}

    def deaggregate(self) -> dict:
        fp = open(self.output["file"], "r")
        out = {}
        for line in fp:
            line = line.rstrip("\r\n").split("\t")
            out[line[0]] = line[1]
        print(out)
        return out
