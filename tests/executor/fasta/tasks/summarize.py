import os
from typing import List

from Bio import SeqIO

from yapm import Task, DependencyInput


class Summarize(Task):
    @staticmethod
    def requires() -> List[str]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": os.path.join(self.wdir, "summary.txt"),
            "final": ["result"]
        }

    def run(self):
        fp = open(self.output["result"], "w")
        print(self.input)
        fp.write(self.record_id + "\t" +
                 str(sum([len(record.seq) for record in SeqIO.parse(self.input["fasta"], "fasta")])))
        fp.write("\n")
        fp.close()
