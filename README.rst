lcmap-ccdc
==========
LCMAP Product Generation

What is lcmap-ccdc?
-----------------------
* Runs change detection & random forest classification at scale
* Built on Apache Spark, Apache Mesos, Docker and Python3
* Runs on 2000 cores as easily as it runs on 1
* Command line interface
* System requirements: Bash & Docker

Get Started
-----------
WIP
.. code-block:: bash

   $ wget https://raw.githubusercontent.com/USGS-EROS/lcmap-ccdc/master/resources/ccdc.install.example -O ccdc.install
   $ emacs ccdc.install
   $ source ccdc.install
   $ ccdc-detect -x -1821585 -y 2891595
   # run detect on neighbor tiles before classifying
   $ ccdc-classify -x -1821585 -y 2891595
   $ ccdc-data     -x -1821585 -y 2891595
   $ ccdc-metadata -x -1821585 -y 2891595

`Frequently Asked Questions <docs/faq.rst>`_
----------------------------------------------

`Roadmap <docs/roadmap.rst>`_
-----------------------------

Developing CCDC
---------------
WIP
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

Development Philosophy
----------------------
Apache Spark is functional programming for cluster computing therefore
CCDC therefore follows functional principles:
data is immutable, functions are the primary unit of abstraction, and functions are  
composed to create higher level functions rather than intermingling (complecting) concepts.
