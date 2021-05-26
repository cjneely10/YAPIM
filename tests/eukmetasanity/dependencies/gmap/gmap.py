import os
from pathlib import Path
from typing import List, Union, Type

from yapim import Task, DependencyInput, prefix
from yapim.utils.config_manager import MissingDataError


class GMAP(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.is_skip:
            sams = []
        else:
            sams = [os.path.join(self.wdir, prefix(transcript)) + ".sam" for transcript in self.get_transcripts()]
        self.output = {
            "sams": sams
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [
            DependencyInput("GMAPBuild")
        ]

    def run(self):
        """
        Run gmap
        """
        # Get transcripts
        for transcript, sam_file in zip(self.get_transcripts(), self.output["sams"]):
            # Generate genome index
            genome_idx = self.input["gmap.build"]["db"]
            _genome_dir = os.path.dirname(str(self.input["gmap.build"]["db"]))
            _genome_basename = os.path.basename(str(self.input["gmap.build"]["db"]))
            # Align
            self.parallel(
                self.program[
                    "-D", _genome_dir, "-d", genome_idx,
                    "-t", self.threads,
                    transcript,
                    (*self.added_flags)
                ] > str(sam_file)
            )

    def get_transcripts(self) -> List[str]:
        """ Parse transcriptome file into list of rna pairs to analyze

        :return: List of transcriptomes
        """
        _path = str(Path(self.config["transcriptome"]).resolve())
        if not os.path.exists(_path):
            raise MissingDataError("Input transcriptome mapping file not found!")
        file_ptr = open(_path, "r")
        _id = self.record_id.replace("-mask", "")
        for line in file_ptr:
            if _id in line:
                return line.rstrip("\r\n").split("\t")[1].split(",")
        return []
