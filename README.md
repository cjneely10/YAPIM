![example workflow](https://github.com/cjneely10/YAPIC/actions/workflows/ci.yml/badge.svg)

# Yet Another PIpeline (Manager)

## Getting Started/Installation 

```shell
conda create --name yapim-demo python=3.8 
conda activate yapim-demo  
pip install git+https://github.com/cjneely10/YAPIM.git@v0.1.2
```

## Documentation

[https://cjneely10.github.io/YAPIM/](https://cjneely10.github.io/YAPIM/)


# Quick usage

### Creating and running a pipeline

See the [tutorial](https://github.com/cjneely10/YAPIM/tree/main/demo).

### Pipeline utils

#### Clean

Delete task by id and clean affected downstream tasks.

```shell
yapim clean -p /path/to/pipeline-directory id1 id2 ...
```

#### Remove

Remove input by id

```shell
yapim remove -p /path/to/pipeline-directory id1 id2 ...
```

------

# About

Created with bioinformatics workflows on very large datasets in mind, Yet Another PIpeline Manager (yapim) seeks to automate the concurrent execution of long-running programs across an input set (i.e., parallelization). Because this process can be cumbersome and error-prone, we have developed an API to optimize this process while maintaining portability, extendibility, and modularity. Yapim is built entirely using Python and is implemented using simple object-oriented programming (OOP) concepts that allow for further extension and development of the API.

YAPIM supports workstation/server usage and High Performance Computing systems that use SLURM.

YAPIM creates its pipelines by generating dependency graphs for a set of Python classes that implement the provided
`Task` or `AggregateTask` abstract base classes. For a given input set, tasks are topologically sorted and automatically 
parallelized to the user-provided cpus/threads/nodes and memory.

### Why use it? 

Many pipeline development tools exist today that offer many options for developers. Yapim was developed with bioinformatics software engineers in mind – its core features focus on: 

- Sandboxing local environments to allow for bash script-like development 
- Providing the ability to reuse existing tools in future pipeline with minimal-to-no code changes 
- Automating data flow between “tasks” in a data pipeline 
- Developing reusable and portable analyses that can directly transfer between local workstations and High-Performance Computing (HPC) systems 
- Automate data management tasks – logging, packaging output, error management, etc.

## Top-level API overview 

A yapim data analysis pipeline operates on user-defined Python classes that inherit from the internally-defined `BaseTask` class and parses them into a simple dependency graph based on the requirements that are defined within each class. Each task also has an expected output, and this output is validated and forwarded to tasks that depend on this information to run.

The `BaseTask` class is further subclassed into two types – the `Task` class and the `AggregateTask` class, with the former operating on each input item individually, and the latter operating on the entire input set at once.

At runtime, the dependency graph is topologically sorted into a list of tasks to complete for a set of inputs. The internally-defined `InputLoader` class provides logic to populate this input set, which can consist of any type that defines the `__str__()` method. Yapim is packaged with a focus on biologically-relevant files and thus provides a default `ExtensionLoader` that loads input from local storage, but this is easily extendible to other input and file types.

Pipeline users can modify the total available resources, as well as the task-specific resources, via a provided configuration file. This information is used to automatically parallelize the pipeline across the input set. This implementation also allows a single pipeline to be applied to multiple similar analyses, and allows for more complex analysis operations without any need to modify written code.

Based on the resources available, the provided Executor class will launch threads that operate on the input set to complete the entire list of blueprints on each item in the input set. The implementation scales to very large input sizes (>100,000 input items) without overwhelming CPU or memory resources. Additionally, this implementation is immediately available to run on either local workstations or SLURM-managed HPC systems, or both.

As items from the input set complete each task, valid output will be automatically copied to a separate output directory. This allows for fast transfer of completed results between workstations, as well as the ability to use results generated by one pipeline as output into another pipeline.

## Task lifecycle and methods 

The `Task` and `AggregateTask` classes follow a specific lifecycle that affords internal dependency graph generation and output accumulation.  (Note: Below, “start time” refers to when a yapim pipeline is first launched). 

- `requires()`: `Task`s define a list of other `Task` classes that provide direct input. This method is used to provide an initial topological sort.
- `depends()`: Yapim’s modular `Task` design allows for a `Task`/`AggregateTask` to be reused as dependencies in other `Task`s. For example, a `Task` may be run multiple times at different points in a data analysis pipeline on newly-identified information, or a wrapper `Task` may expose a program that is used in multiple pipelines. A dependency graph is also generated in the case of dependencies depending on other dependencies. A `Task` may only depend on other `Tasks`, and an `AggregateTask` may only depend on other `AggregateTasks`.
- `__init__(*args, **kwargs)`: Once the pipeline is parsed into a list of tasks to complete, the yapim executor will begin using the defined `Task`/`AggregateTask` class blueprints to complete the analysis pipeline.
    - `super().__init__(*args, **kwargs)`: After calling the superclass initializer, all attributes are available for use, such as output, input, wdir, etc. (link to documentation).
    - `self.output`: Within the initializer, `Task`s will typically define expected output. A `Task` can output any Python object. Any `str` or `Path` type variable will be validated as a file path and confirmed as output for this task (unless wrapped with the provided helper `Result` class). Additionally, the `Task` may define output that will be copied to a separate output directory.
- `run()`: The run method contains logic to run any programs, functions, etc., that may be needed to generate the output defined previously. After the run method is called, the yapim executor will confirm that any paths in the previously defined output now exist. Finally, this output is used to update the internal input state. Any tasks that occur after this point can now reference this data. 
- `deaggregate()`: After an `AggregateTask` completes its run method, it will de-structure its results into individual dictionaries based on the results it generated. This can either be a simple update for all existing input, or can act as a filter to remove input from downstream processing that does not match filter criteria.

There are two ways to stop a Task's lifecycle prior to calling the `run()` method:

1. Skip the task in the configuration file.
2. Define the condition() method to only run a Task if a given boolean condition is first satisfied.

Aside from lifecycle methods, classes that inherit from `Task`/`AggregateTask` have a multitude of helper methods available to easily interface with a local system and apply external software, as well as to immediately distribute large computations to HPC systems based on user settings. 

## Project directory structure

At start time, yapim generates a project directory structure that resembles: 

```shell
|--out 
   |--input 
   |--wdir 
   |--results 
```

During runtime, intermediary files and results are stored in wdir. As tasks complete, designated output files are automatically copied to their corresponding subdirectory within the results output directory.

Yapim is packaged with an eponymous script that handles key features involved in using yapim pipelines: 

- `create`: Once a pipeline is written, this module will confirm the pipeline is valid (e.g., not cyclic, not referencing any programs or Tasks that do not exist, etc.). Upon confirmation, a YAML-formatted configuration file is generated that can be modified to fit the pipeline specifications (see tutorial for more information). 
- `run`: Once a pipeline is created, this module will launch the pipeline. 
- `clean`: This module allows users to delete output stored from a given step. This will also delete any task that is directly affected by the output of this task. 
- `remove`: This module deletes stored input data by id from a pipeline’s internal storage.

## Licensing

<a rel="license" href="http://creativecommons.org/licenses/by-nc/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc/4.0/">Creative Commons Attribution-NonCommercial 4.0 International License</a>.