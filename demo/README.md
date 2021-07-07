# Simple tutorial for creating a data workflow

The following demo will provide an introduction to the features available within the YAPIM API. We will be developing a very simple pipeline to find proteins in bacterial genomes/metagenome-assembled-genomes and to provide functional annotation of genomes that pass a quality check step. This three-part tutorial is succeeded by a larger 7-part tutorial for more intermediate users who may wish to learn about the more complex features available.

The complete code generated in this tutorial is available in this directory (i.e. the `tasks` directory and `tasks-config.yaml` file).

### Step 0: Prepare working environment 

While not directly required, we highly suggest building your pipeline within its own environment. Not only will this prevent dependency-related bugs from occurring during the development process, but this will also make your pipeline system-independent (at least mostly). 

Here, we use miniconda to accomplish this, and start by creating a new environment and installing YAPIM: 

```shell
conda create --name yapim-demo python=3.8
conda activate yapim-demo
pip install git+https://github.com/cjneely10/YAPIM.git
``` 

As we develop our pipeline and add programs, using conda will make ensuring portability painless!  

Also, conda has a handy “export” command to capture our current environment in a format that will allow us to install it on a separate computer, if needed. 

```shell
conda env export > environment.yml
```

With our environment set up, we can now set up our project directory. Create a working directory named “demo” and create an enclosed pipeline package named “tasks,” as shown below: 

```shell
mkdir demo && cd demo 
mkdir tasks 
touch tasks/__init__.py
```

The code that we write in upcoming tutorials will be stored in the `demo/tasks` directory and be a part of its eponymous python package. 

## Your first YAPIM workflow

Let's plan our data pipeline and consider tools we can use to accomplish these steps.

1. Identify proteins in the input genome
    1. This can be completed using `prodigal`:
        1. `conda install -c bioconda prodigal`
2. Determine the quality of each assembly
    1. `checkm` provides this feature:
        1. See [the installation wiki](https://github.com/Ecogenomics/CheckM/wiki/Installation) for dependency download information
        2. `pip install numpy matplotlib pysam`
        3. `pip install checkm-genome`
3. Annotate proteins with PFam
    1. We will use `mmseqs` here:
        1. `conda install -c bioconda mmseqs2`
    2. The PFam database can be downloaded directly:
        1. `mmseqs databases --remove-tmp-files --threads 16 Pfam-A.full pfam_db tmp`

## Step 1: Identify proteins in each input genome

The primary logic for creating a running a YAPIM pipeline will be enclosed in classes we write that inherit from `Task` or `AggregateTask`.  

We begin be creating a new file named `identify_proteins.py` inside of our `tasks` directory.  In this file we will create our first class, which we will name `IdentifyProteins`, and have it inherit from `Task`. Because `Task` is an abstract class, we must provide definitions for any abstract methods it contains.

```python
from typing import List, Union, Type

from yapim import Task, DependencyInput


class IdentifyProteins(Task):
    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        pass
```

Note that type annotations are not directly required for Python, but they are a helpful sanity check, particularly for users running IDEs like PyCharm. 

As alluded to in the `Task` Lifecycle and Methods section, we need to define any tasks that `IdentifyProteins` needs completed to run, and we need to define how `IdentifyProteins` will run itself, along with any output it might generate. 

Since we want this to be the first step in the pipeline, this task will require no other task to be completed before it, so we leave the definition of the `requires()` method as is. We can also ignore the `depends()` method for now, as this will be covered in a subsequent set of tutorials.

#### Calling a program from within YAPIM:

YAPIM makes heavy use of the python library plumbum, which provides an incredibly useful API to run CLI-like commands from within python code. A typical CLI invocation using plumbum resembles:

```python
local["echo"]["Hello", "world!"]()
```

The above command runs the unix program `echo` from the local system with the provided arguments. From within our class, we can invoke the program assigned to this task by using:

```python
self.local["echo"]["Hello", "world!"]()
self.program["Hello", "world!"]()  # Alias for above command if set in configuration file
```

#### Using helper methods and attributes in `Task`

Let’s use this opportunity to explore a bit more of the API. YAPIM provides several helper attributes and methods that allow us to simplify and automate much of the calling code: 

```
.wdir => the working directory in which this task is running 

.threads => number of threads available to launch the program. 

.input => a dictionary of all input that this task was launched with. See the InputLoader section for more information. 

.output => a dictionary defining all of the output that we expect to be generated by this program after the run() method completes. 

.added_flags() => extra flags that are to be passed to the program. 

.flags_to_list(name) => return a configuration file section as a list. 

.parallel(cmd) => parallelize a command across potentially multiple compute nodes.

.single(cmd) => parallelize a command across potentially multiple compute nodes and allot only one thread per call.
```

For a complete list of the methods and attributes available to YAPIM tasks, see the provided documentation.

#### Finishing our implementation

Let us define the command that will call `prodigal`:

```python
from typing import List, Union, Type

from yapim import Task, DependencyInput


class IdentifyProteins(Task):
    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.single(
            self.program[
                "-i", self.input["fasta"],
                "-a", self.wdir.joinpath("proteins.faa"),
                (*self.added_flags)
            ]
        )
```

From the `prodigal` documentation, we can output proteins in FASTA format using the `-a` flag. We pass as input the FASTA file that was provided at runtime, and we set the `-a` flag to output our results to the file names `proteins.faa`, which is stored in the working directory for this `Task`.

This is great, but so far all we have done is run `prodigal`. Ideally, we want output from this step to be available to downstream analysis steps. To do this, we can define the expected output of this task within its initializer:

```python
from typing import List, Union, Type

from yapim import Task, DependencyInput


class IdentifyProteins(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "proteins": self.wdir.joinpath(self.record_id + ".faa"),
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.single(
            self.program[
                "-i", self.input["fasta"],
                "-a", self.output["proteins"],
                (*self.added_flags)
            ]
        )
```

After calling its superclass initializer, we can define the expected output that this task will generate.

**Any data that is of `str` or `Path` type will be validated for existence after the `Task` completes its `run()` method**.

Here, we have listed that this task will output a file in its working directory, and this will be checked when the `Task` completes. Finally, as a matter of convenience, we replaced the hard-coded output value in the `run()` method with its respective value in `self.output`.

### Where did `self.input["fasta"]` come from??

When launched, YAPIM begins by populating the input that will be used to launch this pipeline. Advanced users may see the `InputLoader` section of the documentation.

For our tutorial, note that all input loaded from running YAPIM via its CLI will populate into the following categories based on the file's extension

```
fasta: .fna, .faa, .fasta, .fa
fastq_1: _1.fastq, _1.fq, .1.fastq, .1.fq
fastq_2: _2.fastq, _2.fq, .2.fastq, .2.fq
gff3: .gff3, .gff
```

So, in the case of an input directory containing the following files:

```
-- directory
   |-- genome1.fna
   |-- genome1.gff3
   |-- SRR1234.1.fq
   |-- SRR1234.2.fq
```

would populate into the following input data:

```python
{
    "genome1": {
        "fasta": "genome1.fna",
        "gff3": "genome1.gff3"
    },
    "SRR1234": {
        "fastq_1": "SRR1234.1.fq",
        "fastq_2": "SRR1234.2.fq"
    }
}
```

This implementation allows any class that inherits from `Task` to operate on each individual key:value pair, whereas classes that inherit from `AggregateTask` can operate on the entire input set.

## Testing step 1

With our first step written, it may be useful to test it and ensure that it completes as expected. We should also use this opportunity to generate a config file.

From the command line, we can validate our code and also generate a config file using the command `yapim create`. From within the `demo` directory, run:

```shell
yapim create -t tasks
```

A new directory named `tasks-pipeline` will generate containing the file `tasks-config.yaml`:

```yaml
---  # document start

###########################################
## Global settings
GLOBAL:
  # Maximum threads/cpus to use in analysis
  MaxThreads: 10
  # Maximum memory to use (in GB)
  MaxMemory: 100

###########################################
## SLURM run settings
SLURM:
  ## Set to True if using SLURM
  USE_CLUSTER: false
  ## Pass any flags you wish below
  ## DO NOT PASS the following:
  ## --nodes, --ntasks, --mem, --cpus-per-task
  --qos: unlim
  --job-name: EukMS
  user-id: uid

###########################################
## Pipeline input section
INPUT:
  root: all

###########################################

IdentifyProteins:
  # Number of threads task will use
  threads: 1
  # Amount of memory task will use (in GB)
  memory: 8
  time: "4:00:00"
...  # document end
```

This file represents the primary interface for users to modify calling parameters to your pipeline. For example, in the `GLOBAL` section, a user can set the maximum allowable threads and memory that this pipeline can use. Users may also set their `SLURM` user and partition settings, and can adjust the input to this pipeline.

Users may also list the number of threads and amount of memory to provide to a particular `Task`. When running, YAPIM will automatically schedule its jobs to match resource limitations that are listed using `threads` and `memory`, so be sure these are accurate. This info is also used to launch SLURM jobs, if set.

Let's fill in the section related to the class we just wrote:

```yaml
...
IdentifyProteins:
  # Number of threads task will use
  threads: 1
  # Amount of memory task will use (in GB)
  memory: 8
  time: "4:00:00"
  program: prodigal
  FLAGS:
    -p single
...
```

Here, we define the item `program` (which we had used in our implementation above), and we create a field to allow users to pass flags to this program.

### Configuration reserved keywords

We can access any value within our `Task`'s configuration section using `self.config[name]`.

You may have noticed that several keywords in the `IdentifyProteins` configuration section were automatically available as class attributes. 

These keywords have shortcuts:

```shell
threads: self.threads  # Number of threads as string
memory: self.memory  # Memory as string
FLAGS: self.added_flags  # FLAGS parsed to list
data: self.data  # Data section parsed to list
skip: self.is_skip  # Boolean if task is set to skip
program: self.program  # Local program object
```

In the final case, `self.program` returns to actual program to call, not just the string value (e.g. it returns `self.local[self.config["program"]]`).

### Running the pipeline

We may now run our pipeline on a set of genomes. For this demo, we have provided a set of genomes in the `data` folder enclosed in this demo's directory.

Edit the top of the configuration file to provide settings that match your system. Set the `USE_CLUSTER` flag in the `SLURM` section if running on an HPC. 

Run a YAPIM pipeline using the command:

```shell
yapim run -p tasks-pipeline -c tasks-pipeline/tasks-config.yaml -i ../data
```

This command will run our pipeline and, by default, generate output in the `out` directory.

------

## Step 2: Perform quality analysis on each assembly

With proteins identified in each genome, we would like to filter out any low-quality assemblies. To do this, we will use the program `CheckM`. Unlike the previous `Task` which ran `prodigal` on each genome individually, `CheckM` runs on the entire input set. To handle running programs such as these, YAPIM provides the `AggregateTask` class.

Let's create a new class to handle running `CheckM`. Create a file named `quality_check.py` within the `tasks` directory. In this file, provide the following definition:

```python
from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput


class QualityCheck(AggregateTask):
    def deaggregate(self) -> dict:
        pass
    
    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        pass

```

Notice that the same three methods we defined in the `Task` class need to be defined, but there is an additional method named `deaggregate()`. This method allows us to update, filter, or replace the input to the pipeline, which allow classes that extend `AggregateTask` to provide this functionality.

For this `AggregateTask`, we want to use the results from the `IdentifyProteins` step as input to `CheckM`. Per its documentation, we will need to generate a directory of the protein FASTA files and provide this as input to `CheckM`.

Let's fill out this information below. First, define the required input for this `AggregateTask` in the `requires()` method. In the `run()` method, we provide logic to copy the files to a single directory. Then, we run `CheckM`, and finally delete the temporary directory we created.

We also made us of the `@clean(path)` decorator to remove any existing directory named `tmp` in this `AggregateTask`'s working directory. This is not strictly necessary, but is included as an example of this functionality.

```python
from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput, clean


class QualityCheck(AggregateTask):
    def deaggregate(self) -> dict:
        pass

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
                self.output["outdir"]
            ]
        )

        self.local["rm"]["-r", combined_dir]()
```

We will now implement the `deaggregate()` method to filter our input. If we leave this method blank, then no updates to the original inputs are made. If we provide a return dictionary, then this will update existing data members and remove any ids not present in its returned value. If we call the helper method `self.remap()`, when only this returned data will be present after the filter step.

For our tutorial, we want to filter the input genomes based on the `CheckM` results, which were written to `stdout` by CheckM, and which the YAPIM API automatically saves to the `Task`'s working directory.

Let's write a method to create a generator over our results file, and let's use this generator to write our `deaggregate()` method.

```python
def checkm_results_iter(self):
    min_quality = float(self.config["min_quality"])
    with open(self.wdir.joinpath("task.log")) as log_file_ptr:
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

def deaggregate(self) -> dict:
    return self.filter(self.checkm_results_iter())
```

In our `checkm_results_iter()` method, we first collect a minimum allowable quality that we will define in our configuration file. Then, we open the results file that was saved to this `Task`'s log file, and begin reading.

In the `deaggregate()` method, we use the helper `self.filter(iterable)` method to return the input keys that are in our filter.

The complete `AggregateTask` file should resemble the following:

```python
from typing import List, Union, Type

from yapim import AggregateTask, DependencyInput, clean


class QualityCheck(AggregateTask):
    def deaggregate(self) -> dict:
        return self.filter(self.checkm_results_iter())

    def checkm_results_iter(self):
        min_quality = float(self.config["min_quality"])
        with open(self.wdir.joinpath("task.log")) as log_file_ptr:
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
```

### deaggregate()

Note that `AggregateTask` has helper methods available for deaggregating output of an `AggregateTask`:

```shell
self.input_ids: self.input.keys()
self.input_values: self.input.values()
self.input_items: self.input.items()
```

We can also create new input items using the `self.remap()` method: 
```python
import os

from yapim import prefix


# This method will reset the input of the pipeline to be the prefixes of all data 
# in a particular path, and will create the "fasta" field in each new data input.
def deaggregate(self) -> dict:
    self.remap()
    return {
        prefix(file): {
            "fasta": file
        }
        for file in os.listdir("/path/to/data")
    }


# This method will update all existing inputs to include the field "uploaded-data"
def deaggregate(self) -> dict:
    return {
        record_id : {"updated-data": "data"}
        for record_id in self.input_ids()
    }


# This method will call the passed function on each input id and only return valid inputs
def deaggregate(self) -> dict:
    return self.filter(lambda record_id: record_id > "a")


# This method accepts some iterable of ids and filters our input ids that are not present in this iterable
def deaggregate(self) -> dict:
        return self.filter(self.checkm_results_iter())
```

------

## Step 3: Annotate assemblies that passed quality filter

We will wrap up this demo by performing a functional annotation of the dataset using `MMseqs2`.

Let's create a new file named `annotate.py` within our `tasks` directory and provide definitions as before. Since this will run on each input individually, we will use the `Task` class to write our definition.

```python
from typing import List, Union, Type

from yapim import Task, DependencyInput


class Annotate(Task):
    @staticmethod
    def requires() -> List[Union[str, Type]]:
        pass

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        pass
```

This `Task` will require the output of `IdentifyProteins` to complete its annotation, but it also will require that `QualityCheck` runs first to filter out low-quality genomes. We can provide this information in the `run()` method.

We will define output for this Task to be the results of the search. 

Finally, we will write the command that calls `mmseqs easy-search`. From its documentation, we must provide the input protein file, the database path, the output path, and a temporary working directory as command-line arguments.

```python
from typing import List, Union, Type

from yapim import Task, DependencyInput


class Annotate(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath("result.m8")
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["IdentifyProteins", "QualityCheck"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.parallel(
            self.program[
                "easy-search",
                self.input["IdentifyProteins"]["proteins"],
                self.data[0],
                self.output["result"],
                self.wdir.joinpath("tmp"),
                "--remove-tmp-files",
                "--threads", self.threads,
                (*self.added_flags),
            ]
        )
```

Since we have defined that this `Task` requires the `IdentifyProteins` task to have completed, we can directly access its output via the input to this class.

We reference `self.data[0]`, so we will need to be sure to define `data` in our configuration file.

The very last thing we want to do is define a set of final output from this pipeline. This step allows YAPIM to package selected output, which can make easy transferring and separating results from intermediate files.

We do that in the class initializer:

```python
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.output = {
        "result": self.wdir.joinpath("result.m8"),
        "proteins": self.input["IdentifyProteins"]["proteins"],
        "final": ["result", "proteins"]
    }
```

Any key that we provide in the list of `final` output will be validated and copied to a final output directory named `results`. Notice that we can only provide keys that are available within this `Task`, so we must "save" the results of `IdentifyProteins` prior to listing it for final storage.

The final complete code is here:

```python
from typing import List, Union, Type

from yapim import Task, DependencyInput


class Annotate(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = {
            "result": self.wdir.joinpath("result.m8"),
            "proteins": self.input["IdentifyProteins"]["proteins"],
            "final": ["result", "proteins"]
        }

    @staticmethod
    def requires() -> List[Union[str, Type]]:
        return ["IdentifyProteins", "QualityCheck"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        pass

    def run(self):
        self.parallel(
            self.program[
                "easy-search",
                self.input["IdentifyProteins"]["proteins"],
                self.data[0],
                self.output["result"],
                self.wdir.joinpath("tmp"),
                "--remove-tmp-files",
                "--threads", self.threads,
                (*self.added_flags),
            ]
        )
```

------

## Step 4: Running and testing our pipeline

At this point, our project directory structure should resemble:

```
|--demo
  |--tasks
    |--annotate.py
    |--quality_check.py
    |--identify_proteins.py
  |--tasks-pipeline
```

Let's regenerate our `tasks-pipeline` output:

```shell
yapim create -t tasks
```

If prompted, select `y` to overwrite the existing configuration file.

This should re-create our pipeline package and regenerate a configuration file in the `tasks-pipeline` directory, which should resemble:


```yaml
---  # document start

###########################################
## Global settings
GLOBAL:
  # Maximum threads/cpus to use in analysis
  MaxThreads: 10
  # Maximum memory to use (in GB)
  MaxMemory: 100

###########################################
## SLURM run settings
SLURM:
  ## Set to True if using SLURM
  USE_CLUSTER: false
  ## Pass any flags you wish below
  ## DO NOT PASS the following:
  ## --nodes, --ntasks, --mem, --cpus-per-task
  --qos: unlim
  --job-name: EukMS
  user-id: uid

###########################################
## Pipeline input section
INPUT:
  root: all

###########################################

IdentifyProteins:
  # Number of threads task will use
  threads: 1
  # Amount of memory task will use (in GB)
  memory: 8
  time: "4:00:00"

QualityCheck:
  # Number of threads task will use
  threads: 1
  # Amount of memory task will use (in GB)
  memory: 8
  time: "4:00:00"

Annotate:
  # Number of threads task will use
  threads: 1
  # Amount of memory task will use (in GB)
  memory: 8
  time: "4:00:00"

...  # document end
```

Let's fill in the configuration file, making sure to provide `program`, `data`, and any other definitions we used in our pipeline:

```yaml
---  # document start

###########################################
## Global settings
GLOBAL:
  # Maximum threads/cpus to use in analysis
  MaxThreads: 10
  # Maximum memory to use (in GB)
  MaxMemory: 100

###########################################
## SLURM run settings
SLURM:
  ## Set to True if using SLURM
  USE_CLUSTER: false
  ## Pass any flags you wish below
  ## DO NOT PASS the following:
  ## --nodes, --ntasks, --mem, --cpus-per-task
  --qos: unlim
  --job-name: EukMS
  user-id: uid

###########################################
## Pipeline input section
INPUT:
  root: all

###########################################

IdentifyProteins:
  # Number of threads task will use
  threads: 1
  # Amount of memory task will use (in GB)
  memory: 8
  time: "4:00:00"
  program: prodigal
  FLAGS:
    -p meta

QualityCheck:
  # Number of threads task will use
  threads: 8
  # Amount of memory task will use (in GB)
  memory: 45
  time: "4:00:00"
  program: checkm
  min_quality: 5.0

Annotate:
  # Number of threads task will use
  threads: 1
  # Amount of memory task will use (in GB)
  memory: 16
  time: "4:00:00"
  program: mmseqs
  FLAGS:
    --cov-mode 0
    -s 5
    -c 0.3
    --split-memory-limit 10G

...  # document end
```

Now, we can run our pipeline:

```shell
yapim run -p tasks-pipeline -c tasks-pipeline/tasks-config.yaml -i ../data
```

After it is complete, all results should be present in the `out/results` directory. There will be an enclosed subdirectory for each YAPIM pipeline that has been run on this input set.
