# lcmap-firebird
... makes LCMAP products.

## What is lcmap-firebird?
* It is a command line interface
* Built on Apache Spark and Docker
* That allows you to make LCMAP products on 2000 cores as easily as you can on 1
* It has a built-in pyspark shell for ad-hoc cluster operations
* And a built-in Jupyter Notebook server for development and analysis
* Firebird includes an Apache Cassandra server to save development results

## What's this you say?
* Yes
* It is both an operations and development environment
* It is *really* parallel in either case

## As a development and analysis environment
* Notebooks can be uploaded or mounted via a volume, but controlled and managed
  externally
* Once ready for primetime, code may be merged into lcmap-firebird directly or
  via library
* Results are savable to Cassandra anywhere: Cloud, cluster, dev server, etc

## The Firebird Spark Python Library
* Obtains input data via HTTP
* Performs massively parallel product generation
* Saves results to Apache Cassandra

## How do I get it?
```bash
   $ wget https://raw.githubusercontent.com/USGS-EROS/lcmap-firebird/master/firebird.install.example -O firebird.install
```

## It wasn't there.  How do I get the latest development version?
```bash
   $ wget https://raw.githubusercontent.com/USGS-EROS/lcmap-firebird/develop/firebird.install.example -O firebird.install
```

## How do I configure it?
```bash
   $ vi firebird.install
```

## How do I install it?
```bash
   $ source firebird.install
```

## How do I use it?
```bash
   $ firebird-save -a 1980-01-01/2017-01-01 -b -1821585,2891595 -p seglength -p ccd -d 2014-01-01
```

## Once more?
```bash
   $ firebird-save --acquired 1980-01-01/2017-01-01 \
                   --bounds -1821585,2891595 \
                   --products seglength \
                   --products ccd \
                   --product_dates 2014-01-01
```

## How do I just run a single point instead?
```bash
   $ firebird-save --acquired 1980-01-01/2017-01-01 \
                   --bounds -1821585,2891595 \
                   --products seglength \
                   --products ccd \
                   --product_dates 2014-01-01
                   --clip
```

## How do I find out what products I can run?
```bash
   $ firebird-products
```

## How do I run a bigger area?
```bash
   $ firebird-save --acquired 1980-01-01/2017-01-01 \
                   --bounds -1791585,2891595 \
                   --bounds -1821585,2891595 \
                   --bounds -1791585,2911595 \
                   --bounds -1821585,2911595 \
                   --products seglength \
                   --products ccd \
                   --product_dates 2014-01-01
```

## How do I run a triangle instead?
```bash
   $ firebird-save --acquired 1980-01-01/2017-01-01 \
                   --bounds -1791585,2891595 \
                   --bounds -1821585,2891595 \
                   --bounds -1821585,2911595 \
                   --products seglength \
                   --products ccd \
                   --product_dates 2014-01-01
                   --clip
```

## Developing Firebird

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
   $ pip install -e .[test,dev]
```

* Run tests
```bash
 $ make spark-lib
 $ make docker-deps-up
 $ make docker-db-test-schema
 $ make tests
 $ make docker-deps-down
```

* Cut a branch, do some work, write some tests, update some docs, push to github

* Build a Docker image to test locally
```bash
    $ vi version.txt
    $ make docker-build
    $ vi firebird.install # point to new version that was just built
```

* Publish the Docker image so it will be available to a cluster
```bash
    $ make docker-push
```

## Development Philosophy
Apache Spark is functional programming for cluster computing therefore
Firebird strives to ensure all of it's code follows functional principles of
using immutable data (where possible), using functions as the primary unit of
abstraction, and composing functions (placing them together) rather than
complecting code (intermingling concepts).
