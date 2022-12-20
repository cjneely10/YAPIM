"""Collect input data from existing YAPIM pipelines"""
import os
import pickle
from collections import defaultdict
from pathlib import Path
from typing import Dict, TypeVar

from yapim.utils.config_manager import ImproperInputSection, ConfigManager

PipelineInput = TypeVar("PipelineInput", str, dict, list)
KeyType = TypeVar("KeyType", dict, str)


class ExistingInputLoader(dict):
    """Parses config-level INPUT section for acceptable data matching requested keys/key-mappings"""
    # Default error message for improper input parsing
    _err = ImproperInputSection("""
INPUT section format should be either:

INPUT:
  pipeline_name:
    - key1
    - key2
    - to_key1: from_key1
    - to_key2: from_key2
      to_key3: from_key3

or:

INPUT:
  pipeline_name: all

or:

INPUT:
  pipeline_name:
    to_key1: from_key1
    to_key2: from_key2
""")

    def __init__(self, input_section: Dict[str, PipelineInput], results_base_dir: str):
        """
        Parse a given config INPUT section for available results

        :param input_section: Loaded mappings from user-provided config file
        :param results_base_dir: Top-level directory for YAPIM pipeline results
        """
        super().__init__()
        self._input_section = input_section
        self._results_base_dir = results_base_dir

    @staticmethod
    def _load_pkl_data(pkl_file: Path) -> Dict[str, Dict]:
        with open(pkl_file, "rb") as file_ptr:
            return pickle.load(file_ptr)

    def _parse_dict(self, requested_input: Dict[str, str], pkl_data: Dict[str, Dict], requested_pipeline_id: str):
        pkl_input_data = defaultdict(dict)
        for _to, _from in requested_input.items():
            # Check if pipeline pkl data records are needed in this pipeline
            required = False
            for record_id, pkl_record_data in pkl_data.items():
                if _from in pkl_record_data.keys():
                    pkl_input_data[record_id][_to] = pkl_record_data[_from]
                    required = True
            if not required:
                raise ImproperInputSection(f"INPUT `{requested_pipeline_id}.{_from}` does not have data")
        for key, val in pkl_input_data.items():
            if key not in self.keys():
                self[key] = {}
            self[key].update(val)

    def _parse_str(self, requested_input: str, pkl_data: Dict[str, Dict], requested_pipeline_id: str):
        pkl_input_data = defaultdict(dict)
        required = False
        for record_id, pkl_record_data in pkl_data.items():
            if requested_input == "all":
                pkl_input_data[record_id].update(pkl_record_data)
                required = True
            elif requested_input in pkl_record_data.keys():
                pkl_input_data[record_id][requested_input] = pkl_record_data[requested_input]
                required = True
        if not required:
            raise ImproperInputSection(f"INPUT `{requested_pipeline_id}.{requested_input}` does not have data")
        for key, val in pkl_input_data.items():
            if key not in self.keys():
                self[key] = {}
            self[key].update(val)

    def _parse_list(self, requested_input: [KeyType], pkl_data: Dict[str, Dict], requested_pipeline_id: str):
        for req_input in requested_input:
            if isinstance(req_input, str):
                self._parse_str(req_input, pkl_data, requested_pipeline_id)
            elif isinstance(req_input, dict):
                self._parse_dict(req_input, pkl_data, requested_pipeline_id)
            else:
                raise ExistingInputLoader._err

    def populate(self) -> Dict[str, Dict]:
        """
        Parse INPUT section for data and create input dictionary

        :return: Mappings of {record_id: {}} representing input collected from existing YAPIM pipelines
        """
        requested_pipeline_id: str
        requested_pipeline_id: PipelineInput
        for requested_pipeline_id, requested_pipeline_input in self._input_section.items():
            # Root definition should not be required to be input, but is kept for legacy reasons
            if requested_pipeline_id == ConfigManager.ROOT:
                continue
            # Enclosed .pkl file and data
            try:
                pkl_data = ExistingInputLoader._load_pkl_data(
                    Path(os.path.dirname(self._results_base_dir)).joinpath(requested_pipeline_id).joinpath(
                        requested_pipeline_id + ".pkl"))
            except FileNotFoundError as f_err:
                raise ImproperInputSection(f"Requested pipeline {requested_pipeline_id} is not present "
                                           f"or is improperly formatted") from f_err
            # Definition is {"to": "from"} mappings
            if isinstance(requested_pipeline_input, dict):
                self._parse_dict(requested_pipeline_input, pkl_data, requested_pipeline_id)
            # Definition is a single key to collect
            elif isinstance(requested_pipeline_input, str):
                self._parse_str(requested_pipeline_input, pkl_data, requested_pipeline_id)
            # Definition is a list of keys or {"to": "from"} mappings to collect
            elif isinstance(requested_pipeline_input, list):
                self._parse_list(requested_pipeline_input, pkl_data, requested_pipeline_id)
            else:
                raise ExistingInputLoader._err
        return dict(self)
