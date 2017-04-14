# lcmap-firebird
Apache Spark based product generation for LCMAP.

## Planned Features
* Run and retrieve results and job status for all LCMAP products from the command line.
* Run jobs synchronously or asynchronously.
* Retrieve model inputs from LCMAP HTTP services or local files, determine availability, generate status reports.
* Save science model outputs to Apache Cassandra, local files, or stdout.
* Connect to remote, local or embedded Spark clusters.
* Execute Jupyter Notebooks inside the Firebird Docker image using the same libraries as the ops system.

## Setup

* Install Docker

* Install Conda (works with miniconda3 version 4.3.16)

* Create and activate a conda environment, install hylang (not yet available in conda-forge or pypi).
```bash
   $ conda config --add channels conda-forge 
   $ conda create --name firebird --file environment.txt
   $ source activate firebird
   $ pip install git+https://github.com/hylang/hy.git .
```

## Usage
TODO

## Implementation Wants
* All code should be written according to functional principles... immutability, purity, substitution, statelessness.
