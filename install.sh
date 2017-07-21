#!/usr/bin/env bash

# export variables that are needed by the Docker image.
export FIREBIRD_AARDVARK=http://localhost:5678
export FIREBIRD_CASSANDRA_CONTACT_POINTS=127.0.0.1,127.0.1.1
export FIREBIRD_CASSANDRA_USER=cassandra
export FIREBIRD_CASSANDRA_PASS=cassandra
export FIREBIRD_CASSANDRA_KEYSPACE=lcmap_changes_local
export FIREBIRD_INITIAL_PARTITION_COUNT=1
export FIREBIRD_PRODUCT_PARTITION_COUNT=1
export FIREBIRD_STORAGE_PARTITION_COUNT=1
export FIREBIRD_LOG_LEVEL=WARN

#ts = datetime.datetime.now().isoformat()
#conf = (SparkConf().setAppName("lcmap-firebird-{}".format(ts))
SPARK_EXECUTOR_IMAGE=lcmap-firebird:2017.04.25
BASE="docker run --network=host -it --rm  $SPARK_EXECUTOR_IMAGE"

alias firebird-version="$BASE firebird show version"
alias firebird-products="$BASE firebird show products"
alias firebird-algorithms="$BASE firebird show algorithms"
alias firebird-notebook="$BASE jupyter --ip=$HOSTNAME notebook"
alias firebird-shell="$BASE /bin/bash"

# Spark runtime configuration options are available at
# https://spark.apache.org/docs/latest/configuration.html
#
# This constrains the number of cores Spark requests from Mesos
# (or a standalone cluster).  If unset it asks for all of them.
# --conf spark.cores.max=500
#
alias firebird-save="$BASE spark-submit \
                           --conf spark.app.name='A clever name with timestamp' \
                           --conf spark.executor.cores=1 \
                           --conf spark.master=mesos://localhost:7077 \
                           --conf spark.mesos.principal=mesos \
                           --conf spark.mesos.secret=mesos \
                           --conf spark.mesos.role=mesos \
                           --conf spark.mesos.executor.docker.forcePullImage=false \
                           --conf spark.mesos.executor.docker.image=$SPARK_EXECUTOR_IMAGE \
                           --conf spark.submit.pyFiles=local:///algorithms/pyccd-v2017.06.20.zip \
                           /app/firebird/cmdline.py save"
