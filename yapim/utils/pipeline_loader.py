import pickle
from pathlib import Path

from yapim import InputLoader, ExtensionLoader
from yapim.utils.package_manager import PackageManager


class PipelineLoader(PackageManager):
    def __init__(self, pipeline_package_directory: Path):
        self._pipeline_directory = pipeline_package_directory

    def validate_pipeline_pkl(self) -> dict:
        pipeline_data: dict
        with open(self._pipeline_directory.joinpath(PipelineLoader.pipeline_file), "rb") as provided_pipeline_file:
            pipeline_data = pickle.load(provided_pipeline_file)
        for key in ("tasks", "dependencies"):
            if key not in pipeline_data.keys() or \
                    (isinstance(pipeline_data[key], Path) and not pipeline_data[key].exists()):
                print(f"Unable to load {key} {pipeline_data[key]}")
                print(f"Re-run yaml config to update pipeline")
                exit(1)
        loader_path = pipeline_data.get("loader")
        if loader_path is None:
            pipeline_data["loader"] = ExtensionLoader
        else:
            loader = PipelineLoader._get_loader(pipeline_data["loader"])
            if not issubclass(loader, InputLoader):
                print(f"Unable to validate loader")
                exit(1)
            pipeline_data["loader"] = loader
        return pipeline_data
