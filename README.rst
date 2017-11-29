lcmap-firebird
==============
LCMAP Science Execution Environment

What is lcmap-firebird?
-----------------------
* LCMAP product generation, development and analysis
* Built on Apache Spark, Apache Mesos, Docker and Python3
* Runs on 2000 cores as easily as it runs on 1

As an operations environment
----------------------------
* Command line interface
* System requirements: Bash & Docker

As a development and analysis environment
-----------------------------------------
* Jupyter Notebooks & Apache Cassandra (included)
* Notebooks uploaded or mounted via a volume 
* Results are savable to Cassandra anywhere: Cloud, cluster, dev server, local, etc
* Comes with the Firebird Spark Python library for working with chips, chip-specs and creating time-series data

Streamlined Research to Operations
----------------------------------
* Development and analysis Notebooks are controlled outside Firebird.
* Code may be moved to operations `by merging it directly into lcmap-firebird or included it via library. <#developing-firebird>`_


Get Started
-----------
.. code-block:: bash

   $ wget https://raw.githubusercontent.com/USGS-EROS/lcmap-firebird/master/firebird.install.example -O firebird.install
   $ emacs firebird.install
   $ source firebird.install
   $ firebird-save -a 1980-01-01/2017-01-01 -b -1821585,2891595 -p seglength -p ccd -d 2014-01-01 

`Frequently Asked Questions <docs/faq.rst>`_
----------------------------------------------

`Roadmap <docs/roadmap.rst>`_
-----------------------------

Developing Firebird
-------------------

* Install Docker, Maven and Conda

* Create and activate a conda environment
.. code-block:: bash

   $ conda config --add channels conda-forge
   $ conda create --name firebird python=3.6 numpy pandas scipy gdal -y
   $ source activate firebird

* Clone this repo, install deps
.. code-block:: bash

   $ git clone git@github.com:usgs-eros/lcmap-firebird
   $ cd lcmap-firebird
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
   $ emacs firebird.install # point to new version that was just built

* Publish the Docker image so it will be available to a cluster
.. code-block:: bash

   $ make docker-push

Development Philosophy
----------------------
Apache Spark is functional programming for cluster computing therefore
Firebird strives to ensure all of it's code follows functional principles:
data is immutable, functions are the primary unit of abstraction, and functional 
composition rather than intermingling concepts (complecting.)

