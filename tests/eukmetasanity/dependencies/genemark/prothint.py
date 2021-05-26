import os
from typing import List, Union, Type

from yapim import Task, DependencyInput, touch


class GeneMarkProtHint(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "hints": self.wdir.joinpath("prothint.gff"),
            "evidence": self.wdir.joinpath("evidence.gff")
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["Evidence"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [
            DependencyInput("MMSeqsFilterTaxSeqDB")
        ]

    def run(self):
        """
        Run gmes.prothint
        """
        touch(str(self.output["hints"]))
        touch(str(self.output["evidence"]))
        try:
            tmp_file = os.path.join(self.wdir, "fasta.tmp")
            (self.local["cat"][
                 str(self.input["Evidence"]["prot"]), str(self.input["MMSeqsFilterTaxSeqDB"]["fastas"][0])
             ] > tmp_file)()
            # Run prothint
            self.parallel(
                self.program[
                    str(self.input["fasta"]),
                    tmp_file,
                    "--workdir", self.wdir,
                    "--threads", self.threads,
                ]
            )
        except:
            return
