import os
from typing import List, Union, Type

from yapim import Task, DependencyInput, prefix


class MetaEuk(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "gff3": os.path.join(self.wdir, self.record_id + ".gff3"),
            "prot": os.path.join(self.wdir, self.record_id + ".faa")
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        """
        Run metaeuk
        """
        if len(self.data) == 0:
            return
        database = self.data[0]
        is_profile = []
        if "p:" in database:
            is_profile.append("--slice-search")
            database = database[2:]
        db_prefix = prefix(database)
        _outfile = os.path.join(self.wdir, "%s_%s" % (self.record_id, db_prefix))
        # Run MetaEuk
        self.parallel(
            self.program[
                "easy-predict",
                str(self.input["fasta"]),
                database,
                _outfile,
                os.path.join(self.wdir, "tmp"),
                "--threads", self.threads,
                (*self.added_flags),
                (*is_profile),
            ]
        )
        # Write in GFF3 format
        self.single(
            self.local["metaeuk-to-gff3.py"][
                str(self.input["fasta"]), _outfile + ".fas", "-o",
                str(self.output["gff3"]),
            ],
            time_override="8:00"
        )
        # Rename output file
        os.replace(_outfile + ".fas", str(self.output["prot"]))
