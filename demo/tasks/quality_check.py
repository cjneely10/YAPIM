from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput, clean


class QualityCheck(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "results": self.wdir.joinpath("task.log")
        }

    def deaggregate(self) -> dict:
        return self.filter(self.checkm_results_iter())

    def checkm_results_iter(self):
        min_quality = float(self.config["min_quality"])
        with open(self.output["results"]) as log_file_ptr:
            line = next(log_file_ptr)
            while "Bin Id" not in line:
                line = next(log_file_ptr)
            next(log_file_ptr)
            for line in log_file_ptr:
                if line.startswith("-"):
                    return
                line = line.split()
                completeness = float(line[-3])
                if completeness < min_quality:
                    return
                yield line[0]

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["IdentifyProteins"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    @clean("tmp")
    def run(self):
        combined_dir = str(self.wdir.joinpath("tmp"))
        self.local["mkdir"][combined_dir]()
        for record_data in self.input_values():
            self.local["cp"][record_data["IdentifyProteins"]["proteins"], combined_dir + "/"]()

        self.parallel(
            self.program[
                "lineage_wf",
                "-t", self.threads,
                "-x", "faa",
                "--genes",
                combined_dir,
                self.wdir.joinpath("out")
            ]
        )

        self.local["rm"]["-r", combined_dir]()
