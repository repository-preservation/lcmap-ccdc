from firebird import datastore
import firebird as fb
import pyspark


def save(conf):
    table = fb.CASSANDRA_JOBCONF_TABLE
    key, value = fb.serialize(conf)

    ps = 'UPDATE {} SET conf = {} WHERE key = {} IF EXISTS;'
         .format(table, value, key)
