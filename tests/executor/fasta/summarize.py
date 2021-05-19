import os

from Bio import SeqIO

from src import Task


class Summarize(Task):
    task_name = "summarize"
    requires = []
    depends = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": os.path.join(self.wdir, "summary.txt"),
            "final": ["result"]
        }

    def run(self):
        fp = open(self.output["result"], "w")
        fp.write(self.record_id + "\t" +
                 str(sum([len(record.seq) for record in SeqIO.parse(self.input["fasta"], "fasta")])))
        fp.write("\n")
        fp.close()
