Is this all the documentation that is available?
------------------------------------------------
For version 0.5, yes.  For version 1.0 proper documentation will be published to readthedocs.org.

What credentials and keys do I need to run Firebird?
----------------------------------------------------

As an operations environment, Firebird runs Spark in Docker on Mesos and saves it's results to Cassandra.  

Set the following items in :code:`install.sh`:

* Mesos keys, credentials, and master url(s)
* Cassandra credentials and contact point url
* LCMAP Aardvark (or LCMAP Chipmunk) urls
* Network access to all deployed services
* Full name and tag of Docker image to use from hub.docker.com.

To control performance levels and resources utilization:

* `INITIAL_PARTITION_COUNT`
* `PRODUCT_PARTITION_COUNT`
* `STORAGE_PARTITION_COUNT`
* `CORES`
* `DRIVER_MEMORY`
* `EXECUTOR_MEMORY`

For the PySpark shell:

* `PYSPARK_CORES`
* `PYSPARK_DRIVER_MEMORY`
* `PYSPARK_EXECUTOR_MEMORY`


What are the verbose options for Firebird?
------------------------------------------
.. code-block:: bash

   $ firebird-save --acquired 1980-01-01/2017-01-01 \
                   --bounds -1821585,2891595 \
                   --products seglength \
                   --products ccd \
                   --product_dates 2014-01-01

Running the example command results in: "ValueError: need at least one array to concatenate"
--------------------------------------------------------------------------------------------
This means there was no data available within the bounds specified.  Find an area that has
data, modify the bounds and try again.  

How do I just run a single point instead?
-----------------------------------------
.. code-block:: bash

   $ firebird-save --acquired 1980-01-01/2017-01-01 \
                   --bounds -1821585,2891595 \
                   --products seglength \
                   --products ccd \
                   --product_dates 2014-01-01 \
                   --clip

How do I find out what products I can run?
------------------------------------------
.. code-block:: bash

   $ firebird-products


You seem to like seglength, ccd and 2014-01-01
----------------------------------------------
.. code-block:: bash

   $ firebird-save --acquired 1980-01-01/2017-01-01 \
                   --bounds -1821585,2891595 \
                   --products curveqa \
                   --product_dates 2010-01-01 \
                   --product_dates 2011-01-01 \
                   --product_dates 2012-01-01 \
                   --product_dates 2013-01-01 \
                   --product_dates 2015-01-01 \
                   --product_dates 2016-01-01 \
                   --product_dates 2017-01-01 \
                   --clip

How do I run a bigger area?
---------------------------
.. code-block:: bash

   $ firebird-save --acquired 1980-01-01/2017-01-01 \
                   --bounds -1791585,2891595 \
                   --bounds -1821585,2891595 \
                   --bounds -1791585,2911595 \
                   --bounds -1821585,2911595 \
                   --products seglength \
                   --products ccd \
                   --product_dates 2014-01-01

How do I run a triangle instead?
--------------------------------
.. code-block:: bash

   $ firebird-save --acquired 1980-01-01/2017-01-01 \
                   --bounds -1791585,2891595 \
                   --bounds -1821585,2891595 \
                   --bounds -1821585,2911595 \
                   --products seglength \
                   --products ccd \
                   --product_dates 2014-01-01 \
                   --clip

I ran a really large area and got out of memory errors.
-------------------------------------------------------
Edit :code:`firebird.install` and add more memory to the executors.  
It is helpful to calculate how much data you will be working with ahead of
time based on your query bounds, acquired range and products.

Keep in mind that each partition of data must fit in memory for an executor.

Where do the results get saved?
-------------------------------
In a table matching the algorithm + version, in a keyspace configured
in :code:`firebird.install`.  Tables and keyspaces must be created before running
Firebird, presumably by Cassandra admins.

If you are running the local Cassandra image, you are the Cassandra admin.
In that case, edit :code:`test/resources/test.schema.setup.cql`
then run :code:`$ make docker-db-test-schema`.

How do I run the included Apache Cassandra server?
--------------------------------------------------
:code:`make deps-up` followed by :code:`make db-schema`.
