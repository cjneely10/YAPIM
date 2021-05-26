import os
from pathlib import Path
from typing import List, Union, Type, Tuple

from yapim import Task, DependencyInput, prefix
from yapim.utils.config_manager import MissingDataError


class Hisat2(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.is_skip:
            sams = []
        else:
            sams = [os.path.join(self.wdir, prefix(pair[0])) for pair in self.get_rna_read_pairs()]
        self.output = {
            "sams": sams
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [
            DependencyInput("Hisat2Build")
        ]

    def run(self):
        """
        Run hisat2
        """
        rna_pairs = self.get_rna_read_pairs()
        for pair in rna_pairs:
            out_prefix = os.path.join(self.wdir, prefix(pair[0]))
            if len(pair) > 1:
                _parse_args = ["-1", pair[0], "-2", pair[1]]
            else:
                _parse_args = ["-U", pair[0]]
            # Align
            self.parallel(
                self.program[
                    "-p", self.threads,
                    "-x", self.input["Hisat2Build"]["db"],
                    (*_parse_args),
                    "-S", out_prefix + ".sam",
                    (*self.added_flags),
                ]
            )

    def get_rna_read_pairs(self) -> List[Tuple[str, ...]]:
        """ Parse rna_seq file into list of rna pairs to analyze

        :return: List of read pairs
        """
        _path = str(Path(self.config["rnaseq"]).resolve())
        if not os.path.exists(_path):
            raise MissingDataError("Input rna_seq mapping file not found!")
        file_ptr = open(_path, "r")
        for line in file_ptr:
            if line.startswith(self.record_id):
                pairs_string = line.rstrip("\r\n").split("\t")[1].split(";")
                return [tuple(pair.split(",")) for pair in pairs_string]
        return []
