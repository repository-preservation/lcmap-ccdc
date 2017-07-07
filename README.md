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

* Install Docker, Maven and Conda

* Create and activate a conda environment
```bash
   $ conda config --add channels conda-forge
   $ conda create --name firebird python=3.6 numpy pandas scipy -y
   $ source activate firebird
```

* Clone this repo, install deps
```bash
   $ git clone git@github.com:usgs-eros/lcmap-firebird
   $ cd lcmap-firebird
   $ pip install -e .
```

## Testing
```bash
 $ make spark-lib
 $ make docker-deps-up
 $ make docker-db-test-schema
 $ make tests
 $ make docker-deps-down
```

## Refreshing Test Data
Occasionally it is necessary to update the test data located under
```
test/resources/data1
```  

There are two functions which can be run from a repl.  After updates the
new test data should be committed to the firebird project.  

Make sure the environment variables are set prior to importing test.

```
>>> import os
>>> os.environ['CHIPS_URL'] = 'http://host/v1/landsat/chips'
>>> os.environ['SPECS_URL'] = 'http://host/v1/landsat/chip-specs'
>>>
>>> from test import data
>>> data.update_specs()
>>> data.update_chips()
```

## Usage - WIP
Configuration via environment variables

| VARIABLE | DEFAULT | Description |
| --- | --- | --- |
| CASSANDRA_CONTACT_POINTS | '0.0.0.0' | Cassandra host IP |
| CASSANDRA_USER | | DB username |
| CASSANDRA_PASS | | DB password |
| CASSANDRA_KEYSPACE | 'lcmap_local' | DB keyspace |
| CASSANDRA_RESULTS_TABLE | 'results' | Name of table to store results |
| SPARK_MASTER | 'spark://localhost:7077' | Spark host |
| SPARK_EXECUTOR_IMAGE | | Docker Image for Spark Executor |
| SPARK_EXECUTOR_CORES | | Cores allocated per Spark Executor |
| SPARK_EXECUTOR_FORCE_PULL | 'false' | Force fresh pull of Docker Image |
| AARDVARK_HOST | 'localhost' | Aardvark host |
| AARDVARK_PORT | '5678' | Aardvark port |
| AARDVARK_TILESPECS | '/landsat/tile-specs' | Tile-specs url |
| BEGINNING_OF_TIME | 723546 | Ordinal date for use in seglength product calculation |
| CCD_QA_BITPACKED  | True | Flag for indicating if Landsat Quality data is bitpacked or not |

=======

## Development
Apache Spark is functional programming for cluster computing therefore firebird strives to ensure all of it's code follows functional principles of using immutable data (where possible), using functions as the primary unit of abstraction, and composing functions (placing them together) rather than complecting code (intermingling concepts).
