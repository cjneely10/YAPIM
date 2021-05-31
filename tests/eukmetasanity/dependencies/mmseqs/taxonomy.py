import os
from copy import deepcopy
from typing import List, Union, Type, Tuple

from yapim import Task, DependencyInput


class MMSeqsTaxonomyParser:
    """ This class holds the results of a taxonomic assignment by varying taxonomic levels

    """

    class TaxonomyAssignmentError(AttributeError):
        """
        Error class for not resolving a requested taxonomic level
        """

    _tax_order = ["superkingdom", "kingdom", "phylum", "class", "order", "superfamily", "family", "genus", "species"]

    @staticmethod
    def new():
        out = {}
        for level in MMSeqsTaxonomyParser._tax_order:
            out[level] = {
                "taxid": None,
                "value": None,
                "score": None
            }
        return out

    @staticmethod
    def assignment(result: dict, level: str) -> dict:
        idx = MMSeqsTaxonomyParser._tax_order.index(level)
        _level = None
        while idx >= 0 and _level is None:
            _level = result[MMSeqsTaxonomyParser._tax_order[idx]]
            idx -= 1
        if _level is None:
            raise MMSeqsTaxonomyParser.TaxonomyAssignmentError("%s not identified!" % level)
        return _level

    @staticmethod
    def get_taxonomy(tax_results_file: str, cutoff: float) -> Tuple[dict, dict]:
        """ Parse taxonomy results into TaxonomyAssignment object for downstream use

        :param tax_results_file: File containing MMseqs taxonomy output summary results
        :param cutoff: Minimum percent of reads to allow for parsing a taxonomic level
        :return: TaxonomyAssignment object containing results
        """
        tax_assignment_out = MMSeqsTaxonomyParser.new()
        found_levels = set()
        _tax_results_file = open(tax_results_file, "r")
        try:
            # Get first line
            _level = ""
            while True:
                line = next(_tax_results_file).rstrip("\r\n").split("\t")
                # Parse line for assignment
                _score, _level, _tax_id, _assignment = float(line[0]), line[3], int(line[4]), line[5].lstrip(" ")
                if _assignment in ("unclassified", "root"):
                    continue
                _level = _level.lower()
                if _score >= cutoff and _level not in found_levels:
                    tax_assignment_out[_level] = {
                        "taxid": _tax_id,
                        "value": _assignment,
                        "score": _score
                    }
                    found_levels.add(_level)
        except StopIteration:
            pass
        _tax_results_file.close()
        amended_dict = deepcopy(tax_assignment_out)
        for level in MMSeqsTaxonomyParser._tax_order:
            if amended_dict[level] == {}:
                amended_dict[level] = MMSeqsTaxonomyParser.assignment(amended_dict, level)

        return tax_assignment_out, amended_dict


class MMSeqsTaxonomy(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "tax-report": os.path.join(self.wdir, "tax-report.txt"),
            "taxonomy": {},
            "taxonomy-actual": {}
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
        self.output["taxonomy"], self.output["taxonomy-actual"] = MMSeqsTaxonomyParser.get_taxonomy(
            self.output["tax-report"], self.config["cutoff"]
        )
