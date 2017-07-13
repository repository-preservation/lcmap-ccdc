#!/usr/bin/env bash

export FIREBIRD_AARDVARK=http://localhost:5678
export FIREBIRD_CASSANDRA_CONTACT_POINTS=127.0.0.1,127.0.1.1
export FIREBIRD_CASSANDRA_USER=cassandra
export FIREBIRD_CASSANDRA_PASS=cassandra
export FIREBIRD_CASSANDRA_KEYSPACE=lcmap_changes_local
export FIREBIRD_DRIVER_HOST=$HOSTNAME
export FIREBIRD_INITIAL_PARTITION_COUNT=1
export FIREBIRD_LOG_LEVEL=WARN
export FIREBIRD_MESOS_USER=mesos
export FIREBIRD_MESOS_PASS=mesos
export FIREBIRD_MESOS_ROLE=mesos
export FIREBIRD_PRODUCT_PARTITION_COUNT=1
export FIREBIRD_SPARK_MASTER=mesos:///localhost:7077
export FIREBIRD_SPARK_EXECUTOR_IMAGE=lcmap-firebird:2017.04.25
export FIREBIRD_SPARK_EXECUTOR_CORES=1
export FIREBIRD_SPARK_EXECUTOR_FORCE_PULL=false
export FIREBIRD_SPARK_TOTAL_EXECUTOR_CORES=1000
export FIREBIRD_STORAGE_PARTITION_COUNT=1

BASE="docker run -it --rm --name fb $FIREBIRD_SPARK_EXECUTOR_IMAGE"

alias firebird-products="$BASE firebird show products"
alias firebird-algorithms="$BASE firebird show algorithms"
alias firebird-notebook="$BASE jupyter --ip=$HOSTNAME notebook"
alias firebird-shell="$BASE /bin/bash"
alias firebird-save="$BASE spark-submit --py-files file:///releases/pyccd/v2017.06.20.zip firebird save"
