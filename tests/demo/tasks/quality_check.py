from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput, clean


class QualityCheck(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "outdir": self.wdir.joinpath("out")
        }

    def deaggregate(self) -> dict:
        return {
            record_id: self.input[record_id]
            for record_id in self.filter_results()
        }

    def filter_results(self):
        min_quality = float(self.config["min_quality"])
        with open(self.wdir.joinpath("task.log")) as log_file_ptr:
            line = next(log_file_ptr)
            while "Bin Id" not in line:
                line = next(log_file_ptr)
            next(log_file_ptr)
            for line in log_file_ptr:
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

    @clean("out")
    def run(self):
        self.parallel(
            self.program[
                "lineage_wf",
                "-t", self.threads,
                "-x", "faa",
                "--genes",
                self.storage_directory,
                self.output["outdir"]
            ]
        )
