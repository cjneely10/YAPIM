from typing import List, Union, Type

from EukMetaSanity import TaxonomyAssignment

from yapim import Task, DependencyInput


class Taxonomy(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "taxonomy": Taxonomy.get_taxonomy(str(self.input["mmseqs.taxonomy"]["tax-report"]),
                                              float(self.config["cutoff"])),
            "final": ["mmseqs.taxonomy.tax-report", "taxonomy"]
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
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        """
        Run
        """
        pass
