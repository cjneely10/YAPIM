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
                self.input[key]["Assemble"]["fasta"]
                for key in self.input_ids()
            ]
        }

    def deaggregate(self) -> dict:
        return {
            key: {
                "CatalogueReads": {
                    "catalogue": self.output["fasta_catalogue"]
                }
            }
            for key in self.input_ids()
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
