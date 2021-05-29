from collections import namedtuple
from typing import List, Union, Type, Optional

from yapim import Task, DependencyInput


class TaxonomyAssignmentError(AttributeError):
    """
    Error class for not resolving a requested taxonomic level
    """


class TaxonomyAssignment:
    """ This class holds the results of a taxonomic assignment by varying taxonomic levels

    """
    Assignment = namedtuple("Assignment", ("value", "tax_id", "score"))

    _tax_order = ["superkingdom", "kingdom", "phylum", "_class", "order", "superfamily", "family", "genus", "species"]

    def __init__(self):
        """ Create empty assignment

        """
        self.superkingdom: Optional[TaxonomyAssignment.Assignment] = None
        self.kingdom: Optional[TaxonomyAssignment.Assignment] = None
        self.phylum: Optional[TaxonomyAssignment.Assignment] = None
        self._class: Optional[TaxonomyAssignment.Assignment] = None
        self.order: Optional[TaxonomyAssignment.Assignment] = None
        self.superfamily: Optional[TaxonomyAssignment.Assignment] = None
        self.family: Optional[TaxonomyAssignment.Assignment] = None
        self.genus: Optional[TaxonomyAssignment.Assignment] = None
        self.species: Optional[TaxonomyAssignment.Assignment] = None

    def assignment(self, level: str, find_next_best: bool = True) -> Optional[Assignment]:
        """ Get Assignment object at given level.
        Returns None is level not found in file, or if level did not exist in file

        If requested, will parse up taxonomy assignment file if requested level is not identified
        and stop at first non-null level

        :param level: Taxonomy level string (e.g. kingdom, order, _class, etc.)
        :param find_next_best: Search for next-best tax assignment if provided level not found
        :return: Assignment object or None
        """
        idx = TaxonomyAssignment._tax_order.index(level)
        if not find_next_best:
            return getattr(self, TaxonomyAssignment._tax_order[idx], None)
        _level = None
        while idx >= 0 and _level is None:
            _level = getattr(self, TaxonomyAssignment._tax_order[idx], None)
            idx -= 1
        if _level is None:
            raise TaxonomyAssignmentError("%s not identified!" % level)
        return _level

    def __repr__(self) -> str:
        """ Return string representation of assignment

        :return: Assignment as string
        """
        return "TaxonomyAssignment(superkingdom={}, kingdom={}, phylum={}, class={}, order={}, superfamily={}, " \
               "family={}, genus={}, species={})".format(
            *[
                str(v.value) if v is not None else "None" for v in (
                    self.superkingdom, self.kingdom, self.phylum, self._class, self.order, self.superfamily,
                    self.family, self.genus, self.species
                )
            ]
        )


class Taxonomy(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "taxonomy": Taxonomy.get_taxonomy(str(self.input["MMSeqsTaxonomy"]["tax-report"]),
                                              float(self.config["cutoff"])),
            "tax-report": self.input["MMSeqsTaxonomy"]["tax-report"],
            "final": ["tax-report", "taxonomy"]
        }

    @staticmethod
    def get_taxonomy(tax_results_file: str, cutoff: float) -> TaxonomyAssignment:
        """ Parse taxonomy results into TaxonomyAssignment object for downstream use

        :param tax_results_file: File containing MMseqs taxonomy output summary results
        :param cutoff: Minimum percent of reads to allow for parsing a taxonomic level
        :return: TaxonomyAssignment object containing results
        """
        tax_assignment_out = TaxonomyAssignment()
        tax_levels = ["superkingdom", "kingdom", "phylum", "class", "order", "superfamily", "family", "genus",
                      "species"]
        taxonomy = {key: None for key in tax_levels}
        tax_assignment_out.kingdom = TaxonomyAssignment.Assignment("Eukaryota", 2759, -1)
        try:
            # Get first line
            _tax_results_file = open(tax_results_file, "r")
            _level = ""
            while True:
                line = next(_tax_results_file).rstrip("\r\n").split("\t")
                # Parse line for assignment
                _score, _level, _tax_id, _assignment = float(line[0]), line[3], int(line[4]), line[5].lstrip(" ")
                if _assignment in ("unclassified", "root"):
                    continue
                if _score >= cutoff and taxonomy.get(_level, None) is None:
                    taxonomy[_level] = TaxonomyAssignment.Assignment(_assignment, _tax_id, _score)
        except StopIteration:
            pass
        for level, assignment in taxonomy.items():
            if level == "class":
                setattr(tax_assignment_out, "_class", assignment)
            else:
                setattr(tax_assignment_out, level, assignment)
        return tax_assignment_out

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["Evidence"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [
            DependencyInput("MMSeqsTaxonomy", {"root": {"fasta": "prot"}})
        ]

    def run(self):
        """
        Run
        """
        pass
