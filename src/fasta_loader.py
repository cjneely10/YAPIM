import os
from pathlib import Path
from typing import Dict, Set

from Bio import SeqIO

from src.utils.input_loader import InputLoader


class FastaLoader(InputLoader):
    def __init__(self, fasta_directory: Path, write_dir: Path, valid_extensions: Set[str]):
        self.fasta_directory = fasta_directory
        self.valid_extensions = valid_extensions
        self.write_directory = write_dir
        if not self.write_directory.exists():
            os.makedirs(self.write_directory)

    def load(self) -> Dict[str, Dict]:
        out = {}
        for file in os.listdir(self.fasta_directory):
            ext = os.path.splitext(file)[1]
            if ext in self.valid_extensions:
                file = os.path.join(self.fasta_directory, file)
                basename = os.path.basename(os.path.splitext(file)[0])
                new_file = os.path.join(self.write_directory, basename + ".fna")
                if not os.path.exists(new_file):
                    records = []
                    for record in SeqIO.parse(file, "fasta"):
                        record.description = ""
                        records.append(record)
                    SeqIO.write(records, new_file, "fasta")
                out[basename] = {"fasta": new_file}
        return out
