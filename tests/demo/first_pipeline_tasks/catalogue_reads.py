from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput


class CatalogueReads(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "fasta_catalogue": self.wdir.joinpath("catalogue.fna.gz")
        }

    def aggregate(self) -> dict:
        return {
            "fasta_files": [
                contigs_file
                for key in self.input.keys()
                for contigs_file in self.input[key]["fasta"]
            ]
        }

    def deaggregate(self) -> dict:
        return {
            key: {
                "catalogue": self.output["fasta_catalogue"]
            }
            for key in self.input.keys()
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["Assemble"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.single(
            self.program[
                self.output["fasta_catalogue"],
                (*self.input["fasta_files"])
            ]
        )
