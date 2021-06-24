import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Tuple, Optional, Callable

from Bio import SeqIO

from yapim.utils.config_manager import ConfigManager
from yapim.utils.input_loader import InputLoader


class ExtensionLoader(InputLoader):
    def __init__(self, directory: Optional[Path], write_dir: Path,
                 extension_mapping: Optional[Dict[Tuple, Tuple[str, Callable]]] = None):
        if extension_mapping is None:
            mapping = {
                (".fna", ".fa", ".fasta", ".faa"): ("fasta", ExtensionLoader.parse_fasta),
                (".faa",): ("fasta", ExtensionLoader.parse_fasta),
                (".fastq", ".fq"): ("fastq", ExtensionLoader.parse_fastq),
                ("_1.fastq", "_1.fq", "_1.fastq", ".1.fq"): ("fastq_1", ExtensionLoader.parse_fastq),
                ("_2.fastq", "_2.fq", "_2.fastq", ".2.fq"): ("fastq_2", ExtensionLoader.parse_fastq),
                (".gff", ".gff3"): ("gff3", ExtensionLoader.copy_gff3),
            }

        else:
            mapping = extension_mapping
        self.extension_mapping = {}
        self._expand_convenience_mapping(mapping)
        self.directory = directory
        self.write_directory = write_dir.joinpath(ConfigManager.STORAGE_DIR)
        if not self.write_directory.exists():
            os.makedirs(self.write_directory)

    def _expand_convenience_mapping(self, mapping: Dict):
        for key_tuple, value_tuple in mapping.items():
            for key in key_tuple:
                self.extension_mapping[key] = value_tuple

    def load(self) -> Dict[str, Dict]:
        out = {}
        print("Populating input...")
        if self.directory is None:
            print("Done")
            return out
        with ThreadPoolExecutor() as executor:
            futures = []
            for file in os.listdir(self.directory):
                for key in self.extension_mapping.keys():
                    if file.endswith(key):
                        futures.append(executor.submit(self._load_file, file, key))
            for future in as_completed(futures):
                result = future.result()
                if result[0] not in out.keys():
                    out[result[0]] = {}
                out[result[0]].update(result[1])
        print("Done")
        return out

    def _load_file(self, file: str, ext: str) -> Tuple[str, dict]:
        file = os.path.join(self.directory, file)
        basename = os.path.basename(file)
        new_file = os.path.join(self.write_directory, basename)
        if not os.path.exists(new_file):
            if ext in self.extension_mapping.keys():
                self.extension_mapping[ext][1](file, new_file)
        return basename, {self.extension_mapping[ext][0]: new_file}

    @staticmethod
    def parse_fasta(file: str, new_file: str):
        records = []
        for record in SeqIO.parse(file, "fasta"):
            record.description = ""
            records.append(record)
        SeqIO.write(records, new_file, "fasta")

    @staticmethod
    def parse_fastq(file: str, new_file: str):
        records = []
        for record in SeqIO.parse(file, "fastq"):
            record.description = ""
            records.append(record)
        SeqIO.write(records, new_file, "fastq")

    @staticmethod
    def copy_gff3(file: str, new_file: str):
        original_file_ptr = open(file, "r")
        new_file_ptr = open(new_file, "w")
        for line in original_file_ptr:
            new_file_ptr.write(line)
        new_file_ptr.close()
        original_file_ptr.close()
