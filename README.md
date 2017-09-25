# lcmap-firebird
Apache Spark based product generation for LCMAP.

## Planned Features
* Run and retrieve results and job status for all LCMAP products from the command line.
* Run jobs synchronously or asynchronously.
* Retrieve model inputs from LCMAP HTTP services or local files, determine availability, generate status reports.
* Save science model outputs to Apache Cassandra, local files, or stdout.
* Connect to remote, local or embedded Spark clusters.
* Execute Jupyter Notebooks inside the Firebird Docker image using the same libraries as the ops system.

## Usage
```bash
   $ firebird-save -a 1980-01-01/2017-01-01 -b -1821585,2891595 -p seglength -p ccd -d 2014-01-01
```
## Setup

* Install Docker, Maven and Conda

* Create and activate a conda environment
```bash
   $ conda config --add channels conda-forge
   $ conda create --name firebird python=3.6 numpy pandas scipy gdal -y
   $ source activate firebird
```

* Clone this repo, install deps
```bash
   $ git clone git@github.com:usgs-eros/lcmap-firebird
   $ cd lcmap-firebird
   $ pip install -e .[test]
```

## Testing
```bash
 $ make spark-lib
 $ make docker-deps-up
 $ make docker-db-test-schema
 $ make tests
 $ make docker-deps-down
```

Occasionally chip and chip spec test data may need to be updated if the source
specifications change. Execute ```data.update_specs()``` and ```data.update_chips()``` from
a repl after setting the ```AARDVARK```, ```AARDVARK_SPECS``` and ```AARDVARK_CHIPS```
environment variables.

```
>>> import os
>>> os.environ['AARDVARK'] = 'http://host:port'
>>> os.environ['AARDVARK_SPECS'] = '/v1/landsat/chip-specs'
>>> os.environ['AARDVARK_CHIPS'] = '/v1/landsat/chips'
>>>
>>> from test import data
>>> data.update_specs()
>>> data.update_chips()
```

## Jupyter Notebooks
```firebird-notebook``` will start a jupyter notebook server and expose it on
port 8888.  The example notebooks in firebird/notebooks are copied into the
Docker image at build time and are available to this server.

In order to persist updates to a notebook, a volume can be mounted
at ```/app/notebook``` via install.sh.

## Configuration

## Running
`alias firebird='docker run --rm --name fb lcmap-firebird:2017.04.25 firebird'``

## Enabling Mesos SSL Client Authentication

## Development
Apache Spark is functional programming for cluster computing therefore firebird strives to ensure all of it's code follows functional principles of using immutable data (where possible), using functions as the primary unit of abstraction, and composing functions (placing them together) rather than complecting code (intermingling concepts).
