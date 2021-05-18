import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Set, Tuple

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
        with ThreadPoolExecutor() as executor:
            futures = []
            for file in os.listdir(self.fasta_directory):
                ext = os.path.splitext(file)[1]
                if ext in self.valid_extensions:
                    futures.append(executor.submit(self._load_file, file))
            for future in as_completed(futures):
                result = future.result()
                out[result[0]] = result[1]
        return out

    def _load_file(self, file: str) -> Tuple[str, dict]:
        file = os.path.join(self.fasta_directory, file)
        basename = os.path.basename(os.path.splitext(file)[0])
        new_file = os.path.join(self.write_directory, basename + ".fna")
        if not os.path.exists(new_file):
            records = []
            for record in SeqIO.parse(file, "fasta"):
                record.description = ""
                records.append(record)
            SeqIO.write(records, new_file, "fasta")
        return basename, {"fasta": new_file}
