# Yet Another PIpeline (Manager)

(This repository is in development and is not ready for general usage)

## About

YAPIM is a pipeline-generation tool for easily creating and running data analysis pipelines.
YAPIM supports workstation/server usage and High Performance Computing systems that use SLURM.

YAPIM creates its pipelines by generating dependency graphs for a set of python classes that implement the provided
`Task` or `AggregateTask` abstract base classes. For a given input set, tasks are topologically sorted and automatically 
parallelized to the user-provided cpus/threads/nodes and memory.


## Installation
YAPIM is available for installation via `pip`:

```shell
pip install git+https://github.com/cjneely10/YAPIM.git 
```


## Quick usage

### Creating a pipeline

See the documentation (pending).

### Running a pipeline

Below are simple instructions for running a YAPIM pipeline that is generated using typical methodologies:

```shell
conda env create -f environment.yml
conda activate "<environment-name>"
yapim run -p /path/to/pipeline-directory -c /path/to/config-file.yml -i /path/to/input-directory
```
