lcmap-ccdc
==========
LCMAP Continuous Change Detection and Classification

What is lcmap-ccdc?
-----------------------
* Runs change detection & random forest classification at scale
* Built on Apache Spark, Apache Mesos, Docker and Python3
* Runs on 2000 cores as easily as it runs on 1
* Command line interface

Example
-------
.. code-block:: bash

   # run changedetection on tile that contains x/y
   $ ccdc-changedetection -x -1821585 -y 2891595
   
   # run classification on tile that contains x/y
   # all neighbor tiles should have changedetection run before classifying 
   $ ccdc-classification -x -1821585 -y 2891595

Documentation
-------------

System Overview
===============
In progress.

Dependencies
============

* lcmap-chipmunk deployed for Analysis Ready Data (ARD) and Auxilliary Data (AUX)
* An Apache Cassandra cluster with read & write access to tables in the target keyspace
* Apache Mesos for scaling the Apache Spark cluster (optional)
* Ability to run Docker locally

Installing
==========
.. code-block:: bash

   # Copy ccdc.install.example to your machine
   $ wget https://raw.githubusercontent.com/USGS-EROS/lcmap-ccdc/master/resources/ccdc.install.example -O ccdc.install
   # Edit the values for input urls, Cassandra, lcmap-ccdc image and Spark parallelism
   $ emacs ccdc.install
   # Include ccdc.install into shell environment
   $ . ccdc.install
   

Configuring
===========
In progress.

Running
=======
In progress.

Tuning
======
In progress.

Developing
==========

* Install Docker, Maven and Conda

* Create and activate a conda environment
.. code-block:: bash

   $ conda config --add channels conda-forge
   $ conda create --name ccdc python=3.6 numpy pandas scipy gdal -y
   $ source activate ccdc

* Clone this repo, install deps
.. code-block:: bash

   $ git clone git@github.com:usgs-eros/lcmap-ccdc
   $ cd lcmap-ccdc
   $ pip install -e .[test,dev]

* Run tests
.. code-block:: bash

   $ make spark-lib
   $ make deps-up
   $ make db-schema
   $ make tests
   $ make deps-down

* Cut a branch, do some work, write some tests, update the docs, push to github

* Build a Docker image to test locally
.. code-block:: bash

   $ emacs version.txt
   $ make docker-build
   $ emacs ccdc.install # point to new version that was just built

* Publish the Docker image so it will be available to a cluster
.. code-block:: bash

   $ make docker-push

Versioning
----------
lcmap-ccdc follows semantic versioning: http://semver.org/

Licensing
---------
This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or distribute this software, either in source code form or as a compiled binary, for any purpose, commercial or non-commercial, and by any means.

In jurisdictions that recognize copyright laws, the author or authors of this software dedicate any and all copyright interest in the software to the public domain. We make this dedication for the benefit of the public at large and to the detriment of our heirs and successors. We intend this dedication to be an overt act of relinquishment in perpetuity of all present and future rights to this software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to http://unlicense.org.

Development Philosophy
----------------------
Apache Spark is functional programming for cluster computing therefore
CCDC therefore follows functional principles:
data is immutable, functions are the primary unit of abstraction, and functions are  
composed to create higher level functions rather than intermingling (complecting) concepts.
