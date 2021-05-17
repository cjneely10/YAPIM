import os
from pathlib import Path

from src import AggregateTask, set_complete


class Merge(AggregateTask):
    task_name = "merge"
    requires = ["write"]
    depends = []

    @set_complete
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "file": self.input["file"],
            "final": ["file"]
        }

    def aggregate(self) -> dict:
        file_path = os.path.join(self.wdir, "aggregate-file.txt")
        paths_file = open(file_path, "w")
        for record_id in self.unmerged_input.keys():
            paths_file.write("\t".join([record_id, str(self.unmerged_input[record_id]["write"]["result"])]))
            paths_file.write("\n")
        paths_file.close()
        return {"file": Path(file_path).resolve()}

    def run(self):
        fp = open(self.input["file"], "r")
        print(fp.readlines())
        fp.close()
