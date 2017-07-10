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

## Configuration
| VARIABLE | DEFAULT | Description |
| --- | --- | --- |
| FB_AARDVARK | 'http://localhost:5678' | Aardvark host:port |
| FB_AARDVARK_SPECS |'/v1/landsat/chip-specs' | Aardvark chip specs resource path |
| FB_AARDVARK_CHIPS | '/v1/landsat/chips' | Aardvark chips resource path |
| FB_CASSANDRA_CONTACT_POINTS | $HOST | Cassandra host IP |
| FB_CASSANDRA_USER | 'cassandra' | DB username |
| FB_CASSANDRA_PASS | 'cassandra' | DB password |
| FB_CASSANDRA_KEYSPACE | 'lcmap_changes_local' | DB keyspace |
| FB_DRIVER_HOST | $HOST | Advertised Hostname from SparkContext to Executors |
| FB_INITIAL_PARTITION_COUNT | 1 | Aardvark query parallelism
| FB_PRODUCT_PARTITION_COUNT | 1 | Product generation parallelism
| FB_STORAGE_PARTITION_COUNT | 1 | Cassandra storage parallelism
| FB_LOG_LEVEL | 'WARN' | Firebird log4j logging level
| FB_MESOS_USER | 'mesos' | Mesos username (only if running on Mesos) |
| FB_MESOS_PASS | 'mesos' | Mesos password (only if running on Mesos) |
| SPARK_MASTER | 'spark://localhost:7077' | Spark host |
| SPARK_EXECUTOR_IMAGE | | Docker Image for Spark Executor |
| SPARK_EXECUTOR_CORES | 1 | Cores allocated per Spark Executor |
| SPARK_EXECUTOR_FORCE_PULL | 'false' | Force fresh pull of Docker Image |
| CCD_QA_BITPACKED  | 'True' | Landsat Quality data bitpacked T/F |

## Enabling Mesos SSL Client Authentication
Mesos authentication over SSL requires additional environment variables,
certificates and keys.  These are not included with the published Docker image
as they are specific to each environment.

To enable ssl authentication, set the following environment variables at
Docker image runtime and volume mount the necessary files.
| VARIABLE | VALUE |
| --- | --- |
|LIBPROCESS_SSL_ENABLED | 1 |
|LIBPROCESS_SSL_SUPPORT_DOWNGRADE | true |
|LIBPROCESS_SSL_VERIFY_CERT | 0 |
|LIBPROCESS_SSL_CERT_FILE | /path/to/mesos.cert |
|LIBPROCESS_SSL_KEY_FILE | /path/to/mesos.key |
|LIBPROCESS_SSL_CA_DIR | /path/to/mesos_certpack/ |
|LIBPROCESS_SSL_CA_FILE | /path/to/cacert.crt |
|LIBPROCESS_SSL_ENABLE_SSL_V3 | 0 |
|LIBPROCESS_SSL_ENABLE_TLS_V1_0 | 0 |
|LIBPROCESS_SSL_ENABLE_TLS_V1_1 | 0 |
|LIBPROCESS_SSL_ENABLE_TLS_V1_2 | 1 |

## Development
Apache Spark is functional programming for cluster computing therefore firebird strives to ensure all of it's code follows functional principles of using immutable data (where possible), using functions as the primary unit of abstraction, and composing functions (placing them together) rather than complecting code (intermingling concepts).
