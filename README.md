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

* Create and activate a conda environment
```bash
   $ conda config --add channels conda-forge 
   $ conda create --name firebird --file environment.txt
   $ source activate firebird
```

## Usage
Configuration via environment variables

| VARIABLE | DEFAULT | Description |
| --- | --- | --- |
| CASSANDRA_CONTACT_POINTS | '0.0.0.0' | Cassandra host IP |
| CASSANDRA_USER | | DB username |
| CASSANDRA_PASS | | DB password |
| CASSANDRA_KEYSPACE | 'lcmap_local' | DB keyspace |
| SPARK_MASTER | 'spark://localhost:7077' | Spark host |
| AARDVARK_HOST | 'localhost' | Aardvark host |
| AARDVARK_PORT | '5678' | Aardvark port |
| AARDVARK_TILESPECS | '/landsat/tile-specs' | Tile-specs url |


* Command line
```bash
```

* Jupyter
```bash
```

## Implementation Wants
* Leverage [hylang](http://docs.hylang.org/en/latest/)
* Utilize xarray for input handling
* All code should be written according to functional principles... immutability, purity, substitution, statelessness.

