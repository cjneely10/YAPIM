"""Helper class for populating input from a directory to match provided extension types"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Tuple, Optional, Callable

from Bio import SeqIO

from yapim.utils.input_loader import InputLoader
from yapim.utils.path_manager import PathManager


class ExtensionLoader(InputLoader):
    """Extends InputLoader to populate input from contents of a directory. Uses provided extension mapping to create
    input data"""
    def __init__(self, directory: Optional[Path], write_dir: Path,
                 extension_mapping: Optional[Dict[Tuple, Tuple[str, Callable]]] = None):
        """ Create ExtensionLoader

        Examples of mapping:

        mapping = {
            (".fna", ".fa", ".fasta", ".faa"): ("fasta", ExtensionLoader.parse_fasta),
            (".faa",): ("fasta", ExtensionLoader.parse_fasta),
            ("_1.fastq", "_1.fq", ".1.fastq", ".1.fq"): ("fastq_1", ExtensionLoader.parse_fastq),
            ("_2.fastq", "_2.fq", ".2.fastq", ".2.fq"): ("fastq_2", ExtensionLoader.parse_fastq),
            (".gff", ".gff3"): ("gff3", ExtensionLoader.copy_gff3),
        }

        :param directory: Path to directory with input data
        :param write_dir: Directory to write output
        :param extension_mapping: Mapping, or default to above example
        """
        if extension_mapping is None:
            mapping = {
                (".fna", ".fa", ".fasta", ".faa"): ("fasta", ExtensionLoader.parse_fasta),
                (".faa",): ("fasta", ExtensionLoader.parse_fasta),
                ("_1.fastq", "_1.fq", ".1.fastq", ".1.fq"): ("fastq_1", ExtensionLoader.parse_fastq),
                ("_2.fastq", "_2.fq", ".2.fastq", ".2.fq"): ("fastq_2", ExtensionLoader.parse_fastq),
                (".gff", ".gff3"): ("gff3", ExtensionLoader.copy_gff3),
            }

        else:
            mapping = extension_mapping
        self.extension_mapping = {}
        self._expand_convenience_mapping(mapping)
        self.directory = directory
        self.write_directory = write_dir.joinpath(PathManager.STORAGE_DIR)
        if not self.write_directory.exists():
            os.makedirs(self.write_directory)

    def storage_directory(self):
        return self.write_directory

    def load(self) -> Dict[str, Dict]:
        out = {}
        print("Populating input...")
        if self.directory is None:
            print("Done")
            return out
        with ThreadPoolExecutor() as executor:
            futures = []
            for file in os.listdir(self.directory):
                # pylint: disable=consider-iterating-dictionary
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

    def _expand_convenience_mapping(self, mapping: Dict):
        """Expand tuple from object initializer to map to name/Callable tuple"""
        for key_tuple, value_tuple in mapping.items():
            for key in key_tuple:
                self.extension_mapping[key] = value_tuple

    def _load_file(self, file: str, ext: str) -> Tuple[str, dict]:
        """Load a file, return its basename (less ext) and the dict mapping its file path"""
        file = os.path.join(self.directory, file)
        basename = os.path.basename(file).replace(ext, "")
        new_file = os.path.join(self.write_directory, basename + ext)
        if not os.path.exists(new_file):
            if ext in self.extension_mapping.keys():
                self.extension_mapping[ext][1](file, new_file)
        return basename, {self.extension_mapping[ext][0]: new_file}

    @staticmethod
    def parse_fasta(file: str, new_file: str):
        """Wrapper SeqIO.parse"""
        SeqIO.write(ExtensionLoader._record_iter(file, "fasta"), new_file, "fasta")

    @staticmethod
    def parse_fastq(file: str, new_file: str):
        """Wrapper SeqIO.parse"""
        SeqIO.write(ExtensionLoader._record_iter(file, "fastq"), new_file, "fastq")

    @staticmethod
    def _record_iter(file: str, file_type: str):
        """Clear description column of records"""
        for record in SeqIO.parse(file, file_type):
            record.description = ""
            yield record

    @staticmethod
    def copy_gff3(file: str, new_file: str):
        """Copy gff3 contents"""
        original_file_ptr = open(file, "r")
        new_file_ptr = open(new_file, "w")
        for line in original_file_ptr:
            new_file_ptr.write(line)
        new_file_ptr.close()
        original_file_ptr.close()
