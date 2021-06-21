from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput


class Cluster(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "cluster_db": str(self.wdir.joinpath("cluster_db")),
            "cluster_fasta": str(self.wdir.joinpath("cluster.fasta"))
        }

    def aggregate(self) -> dict:
        combined_file_path = str(self.wdir.joinpath("proteins.faa"))
        with open(combined_file_path, "w") as combined_proteins_file:
            for record_id, data_dict in self.input.items():
                with open(data_dict["IdentifyProteins"]["proteins"], "r") as protein_file:
                    combined_proteins_file.write(protein_file.read())
        return {"proteins": combined_proteins_file}

    def deaggregate(self) -> dict:
        return {
            "clusters": self.output["cluster_db"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["IdentifyProteins"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
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
                "easy-linclust",
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
                "--threads", self.threads,
            ]
        )
        # Output to FASTA format
        self.single(
            self.program[
                "convert2fasta",
                self.output["cluster_db"],
                self.output["cluster_fasta"],
            ]
        )
