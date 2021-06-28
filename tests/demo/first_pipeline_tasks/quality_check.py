from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput, VersionInfo


class QualityCheck(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "outdir": self.wdir.joinpath("out")
        }

    def versions(self) -> List[VersionInfo]:
        return [VersionInfo(calling_parameter="-h", version="1.1.3")]

    def deaggregate(self) -> dict:
        return {
            record_id: self.input[record_id]
            for record_id in self.filter_results()
        }

    def filter_results(self):
        with open(self.wdir.joinpath("task.log")) as log_file_ptr:
            line = next(log_file_ptr)
            while "Bin Id" not in line:
                line = next(log_file_ptr)
            next(log_file_ptr)
            for line in log_file_ptr:
                line = line.split()
                completeness = float(line[-3])
                if completeness < 5.0:
                    return
                yield line[0]

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["Bin"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.parallel(
            self.program[
                "lineage_wf",
                "-t", self.threads,
                "-x", "fna",
                self.input["Bin"]["bins"],
                self.output["outdir"]
            ]
        )
