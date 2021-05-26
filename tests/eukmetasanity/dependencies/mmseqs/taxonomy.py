import os
from typing import List, Union, Type

from yapim import Task, DependencyInput


class MMSeqsTaxonomy(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "tax-report": os.path.join(self.wdir, "tax-report.txt"),
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [
            DependencyInput("MMSeqsCreateDB")
        ]

    def run(self):
        """
        Run mmseqs.taxonomy
        """
        tax_db = os.path.join(self.wdir, self.record_id + "-tax_db")
        # Search taxonomy db
        self.parallel(
            self.program[
                "taxonomy",
                self.input["MMSeqsCreateDB"]["db"],
                self.data[0],
                tax_db,
                os.path.join(self.wdir, "tmp"),
                (*self.added_flags),
                "--threads", self.threads,
            ]
        )
        # Generate taxonomy report
        self.single(
            self.program[
                "taxonomyreport",
                self.data[0],
                tax_db,
                self.output["tax-report"]
            ],
            "1:00:00"
        )
