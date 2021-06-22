import glob
from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput, Result


class Cluster(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "cluster_db": Result(str(self.wdir.joinpath("cluster_db"))),
            "cluster_msa_db": Result(str(self.wdir.joinpath("cluster_msa_db"))),
            "cluster_fasta": str(self.wdir.joinpath("cluster.fasta")),
            "final": ["cluster_msa_db", "cluster_fasta"]
        }

    def aggregate(self) -> dict:
        combined_file_path = str(self.wdir.joinpath("proteins.faa"))
        with open(combined_file_path, "w") as combined_proteins_file:
            for record_id, data_dict in self.input.items():
                with open(data_dict["IdentifyProteins"]["proteins"], "r") as protein_file:
                    combined_proteins_file.write(protein_file.read())
        return {"proteins": combined_file_path}

    def deaggregate(self) -> dict:
        return {
            "cluster_db": self.output["cluster_db"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["IdentifyProteins"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        if len(glob.glob(str(self.wdir.joinpath("cluster_msa_db*")))) > 0:
            return
        # Create DB
        mmseqs_db = str(self.wdir.joinpath("proteins_db"))
        self.single(
            self.program[
                "createdb",
                str(self.input["proteins"]),
                mmseqs_db
            ]
        )
        # Cluster
        self.parallel(
            self.program[
                "linclust",
                mmseqs_db,
                self.output["cluster_db"],
                str(self.wdir.joinpath("tmp")),
                "--threads", self.threads,
                (*self.added_flags)
            ]
        )
        # Align
        self.parallel(
            self.program[
                "result2msa",
                mmseqs_db,
                mmseqs_db,
                self.output["cluster_db"],
                self.output["cluster_msa_db"],
                "--threads", self.threads,
                "--msa-format-mode", 3
            ]
        )
