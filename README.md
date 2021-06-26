# Yet Another PIpeline (Manager)

(This repository is in development and is not ready for general usage)

## About

YAPIM is a pipeline-generation tool for easily creating and running data analysis pipelines.
YAPIM supports workstation/server usage and High Performance Computing systems that use SLURM.

YAPM creates its pipelines by generating dependency graphs for a set of python classes that implement the provided
`Task` or `AggregateTask` abstract base classes. For a given input set, tasks are topologically sorted and automatically 
parallelized to the user-provided cpus/threads/nodes and memory.


## Installation
YAPIM is available for installation via `pip`:

```shell
pip install git+https://github.com/cjneely10/YAPIM.git 
```


## Quick usage

### Creating a pipeline

YAPIM pipelines consist of a set of classes that inherit from either `Task` or `AggregateTask`, which differ in their 
ability to run a task on a single input item or the entire collection, respectively.

```python
from typing import List

from yapim import Task, AggregateTask, DependencyInput


class SampleTask(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            
        }
    
    @staticmethod
    def requires() -> List[str]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        pass


class SampleAggregateTask(AggregateTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            
        }

    @staticmethod
    def requires() -> List[str]:
        pass
    
    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        pass

    def deaggregate(self) -> dict:
        pass
```

Ordering of tasks is accomplished by providing the name or class object of tasks that must be completed prior to run.



### Generating configuration data

### Run a pipeline - command line

### Run a pipeline - API

#### InputLoader
