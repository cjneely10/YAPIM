# Yet Another PIpeline (Manager)

## Installation
YAPIM is available for installation via `pip`:

```shell
pip install git+https://github.com/cjneely10/YAPIM.git 
```


# Quick usage

### Creating a pipeline

See the [tutorial](https://github.com/cjneely10/YAPIM/tree/main/demo).

### Running a pipeline

Below are simple instructions for running a YAPIM pipeline that is generated using typical methodologies:

```shell
conda env create -f environment.yml
conda activate "<environment-name>"
yapim run -p /path/to/pipeline-directory -c /path/to/config-file.yml -i /path/to/input-directory
```

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

Created with bioinformatics workflows on very large datasets in mind, Yet Another PIpeline Manager (yapim) was developed with the goal of optimizing execution time of long-running programs via concurrent execution across the input set (I.e., parallelization). Moreover, because this process can be cumbersome and error-prone in a research setting, we have developed a simple API to optimize this process and to minimize the amount of boilerplate code needed, while maintaining portability, extendibility, and modularity within not only the yapim code base itself, but within pipelines that will be developed with this toolkit. Yapim is built entirely using Python and is implemented using simple object-oriented programming (OOP) concepts that allow for further extension and development of the API.

YAPIM supports workstation/server usage and High Performance Computing systems that use SLURM.

YAPIM creates its pipelines by generating dependency graphs for a set of python classes that implement the provided
`Task` or `AggregateTask` abstract base classes. For a given input set, tasks are topologically sorted and automatically 
parallelized to the user-provided cpus/threads/nodes and memory.

### Why use it? 

Many pipeline development tools exist today that offer many options for developers. Yapim was developed with researchers and data analysis in mind – its core features focus on: 

- Sandboxing local environments to allow for bash script-like development 
- Providing the ability to reuse existing tools in future pipeline with minimal-to-no code changes 
- Automating data flow between “tasks” in a data pipeline 
- Developing reusable and portable analyses that can directly transfer between local workstations and High-Performance Computing (HPC) systems 
- Automate data management tasks – logging, packaging output, error management, etc.

## Top-level API overview 

A yapim data analysis pipeline operates on user-defined Python classes that inherit from the internally-defined BaseTask class and parses them into a simple dependency graph based on the requirements that are defined within each class. Each task also has an expected output, and this output is validated and forwarded to tasks that depend on this information to run. 

[Infographic showing class, methods, etc.] 

At runtime, the dependency graph is topologically sorted into a list of tasks to complete for a set of inputs. The internally-defined InputLoader class provides logic to populate this input set, which can consist of any hashable input key. Yapim is packaged with a focus on biologically-relevant files and thus provides a default ExtensionLoader that loads input from local storage, but this is easily extendible to other input and file types. 

[Infographic for input loading] 

The BaseTask class is further subclassed into two types – the Task class and the AggregateTask class, with the former operating on each input item individually, and the latter operating on the entire input set at once. Pipeline users can modify the total available resources, as well as the task-specific resources, via a provided configuration file. This information is used to automatically parallelize the pipeline across the input set. This implementation also allows a single pipeline to be applied to multiple similar analyses, and allows for more complex analysis operations without any need to modify written code. 

[Infographic for top-level differences between Task types] 

Based on the resources available, the provided Executor class will launch threads that operate on the input set to complete the entire list of blueprints on each item in the input set. The implementation scales to very large input sizes (>100,000 input items) without overwhelming CPU or memory resources. Additionally, this implementation is immediately available to run on either local workstations or SLURM-managed HPC systems, or both. 

[Infographic showing execution process] 

As items from the input set complete each task, valid output will be automatically copied to a separate output directory. This allows for fast transfer of completed results between workstations, as well as the ability to use results generated by one pipeline as output into another pipeline. 

## Getting Started/Installation 

```shell
conda create --name yapim-demo python=3.8 
conda activate yapim-demo  
pip install git+https://github.com/cjneely10/YAPIM.git 
```

## Task lifecycle and methods 

Yapim operates on the internally-provided BaseTask class, which is further subclassed into the Task and AggregateTask classes. The Task class operates on a single input at a time, whereas the AggregateTask class will collect all input prior to operating, and will de-aggregate its output after operating to update/refresh the input set, if desired. 

The Task class follows a specific lifecycle that affords internal dependency graph generation and output accumulation.  (Note: Below, “start time” refers to when yapim is first started). 

- `requires()`: Tasks define a list of other Task classes that provide direct input. This method is used to provide an initial topological sort.
- `depends()`: Yapim’s modular Task design allows for reuse as dependencies in Tasks. For example, a Task may be run multiple times at different points in a data analysis pipeline on newly-identified information, or a wrapper Task may expose a program that is used in multiple pipelines. A dependency graph is also generated in the case of dependencies depending on other dependencies.
- `super().__init__(*args, **kwargs)`: After calling the superclass constructor, all attributes are available for use, such as output, input, wdir, etc. (link to documentation).
- `__init__()`: Once the pipeline is parsed into a list of tasks to complete, the yapim executor will begin using the defined Task class blueprints to complete the analysis pipeline. 
- `self.output`: Within the initializer, Tasks will typically define expected output. A Task can output any Python object. Any str or Path type variable will be validated as a file path and confirmed as output for this task (unless wrapped with the provided helper Result class). Additionally, the Task may define output that will be copied to a separate output directory. 
- `run()`: The run method is meant to run any programs, functions, etc., that may be needed to generate the output defined previously. After the run method is called, the yapim executor will confirm that the output previously defined exists. Finally, this output is used to update the internal input state. Any tasks that occur after this point can now reference this data. 
- `deaggregate()`: After an AggregateTask completes its run method, it will de-structure its results into individual dictionaries based on the results it generated. This can either be a simple update for all existing input, or can act as a filter to remove input from downstream processing that does not match filter criteria.

Aside from lifecycle methods, classes that inherit from Task/AggregateTask have a multitude of helper methods available to easily interface with a local system and apply external software, as well as to immediately distribute large computations to HPC systems based on user settings. 

## Project directory structure

At start time, yapim generates a project directory structure that resembles: 

     |--out 

       |--input 

       |--wdir 

       |--results 

During runtime, intermediary files and results are stored in wdir. As tasks complete, designated output files are automatically copied to their corresponding subdirectory within the results output directory.

Yapim is packaged with an eponymous script that handles key features involved in using yapim pipelines: 

- `create`: Once a pipeline is written, this module will confirm the pipeline is valid (e.g., not cyclic, not referencing any programs or Tasks that do not exist, etc.). Upon confirmation, a YAML-formatted configuration file is generated that can be modified to fit the pipeline specifications (see tutorial for more information). 
- `run`: Once a pipeline is created, this module will launch the pipeline. 
- `clean`: This module allows users to delete output stored from a given step. This will also delete any task that is directly affected by the output of this task. 
- `remove`: This module deletes stored input data by id from a pipeline’s internal storage.

## Writing reusable Tasks 

The yapim API allows Tasks to access the output of any other Task that is specified within its defined requires() or depends() methods. This information is accessible through the input attribute. While this implementation is useful for Task ordering and atomizing Task operations, it does introduce a degree of hard-coding in that this implementation would require a specific Task to be completed with it. 

To avoid this coupling and hard-coding, we can re-write portions of our above pipeline as dependencies. This refactoring will allow Tasks to be reused in any pipeline so long as the required input parameters that it uses are specified. 

[Demo writing dependency] 

Once all changes are complete, we have made our Task entirely independent of any specific pipeline. From here, we can use it within our pipeline as a Dependency. 

## Using a pre-packaged dependency 

## Writing a custom InputLoader 

Yapim’s Executor class requires an object that extends InputLoader and defines a load() method. This method must return a dictionary of input data, and this dictionary will be passed to the pipeline that the Executor is launching. The provided ExtensionLoader class accomplishes this by collecting all files with the same basename into a dictionary, and matching file extension types to a broader set of biologically-relevant input terms (such as “fasta,” “fastq,” “gff,” etc.). 

Any class that defines a load() method that accomplishes this functionality may be substituted. Here, we will demo a downloader class that can be used to collect data from online prior to launching the pipeline. 