from cytoolz import concat
from functools import partial
from pyspark.sql import Row
from test import TEST_ROOT
from numpy import array
from numpy import int16, uint16

import json

def merge(inputs):
  return list(concat(inputs))

acquired   = '1980-01-01/2017-01-01'
schemabase = ['chipx', 'chipy', 'x', 'y', 'dates']
auxbase    = ['aspect','dem', 'mpw', 'posidex', 'slope']
attrbase   = ['blmag',  'grmag',  'remag',  'nimag',  's1mag',  's2mag',  'thmag',  'blrmse', 
              'grrmse', 'rermse', 'nirmse', 's1rmse', 's2rmse', 'thrmse', 'blcoef', 'grcoef', 
              'recoef', 'nicoef', 's1coef', 's2coef', 'thcoef', 'blint',  'reint',  'niint', 
              's1int',  's2int',  'thint']

ard_schema       = merge([schemabase, ['blues', 'greens', 'reds', 'nirs', 'swir1s', 'swir2s', 'thermals', 'qas']])
aux_schema       = merge([schemabase, auxbase, ['trends']])
features_columns = merge([attrbase,   auxbase, ['grint']])
features_dframe  = merge([attrbase, ard_schema, ['sday', 'eday', 'grint']])
ccd_schema_base  = merge([schemabase, attrbase, ['sday', 'eday', 'bday', 'chprob', 'curqa', 'snprob', 'waprob', 'clprob', 'prmask']])
ccd_format_keys  = merge([ccd_schema_base, ['grint']])
ccd_schema_names = merge([ccd_schema_base, ['rfrawp']])
merged_schema    = merge([ard_schema, aux_schema])

timeseries_element = ((-1815585, 1064805, -1814475, 1062105),
                      {'blues':    array([-9999, -9999, -9999, -9999], dtype=int16),
                       'qas':      array([1, 1, 1, 1], dtype=uint16),
                       'nirs':     array([-9999, -9999, -9999, -9999], dtype=int16),
                       'thermals': array([-9999, -9999, -9999, -9999], dtype=int16),
                       'swir2s':   array([-9999, -9999, -9999, -9999], dtype=int16),
                       'reds':     array([-9999, -9999, -9999, -9999], dtype=int16),
                       'swir1s':   array([-9999, -9999, -9999, -9999], dtype=int16),
                       'greens':   array([-9999, -9999, -9999, -9999], dtype=int16),
                       'dates':    [734973, 731205, 724404, 723868]})

grid_resp = json.loads(open(TEST_ROOT+"/data/grid_response.json").read())
near_resp = json.loads(open(TEST_ROOT+"/data/near_response.json").read())
snap_resp = json.loads(open(TEST_ROOT+"/data/snap_response.json").read())
tile_resp = json.loads(open(TEST_ROOT+"/data/tile_response.json").read())
regy_resp = json.loads(open(TEST_ROOT+"/data/registry_response.json").read())

merlin_grid_partial = partial(lambda: grid_resp)
merlin_near_partial = partial(lambda x, y: near_resp)
merlin_snap_partial = partial(lambda x, y: snap_resp)
merlin_regy_partial = partial(lambda: regy_resp)

def faux_dataframe(ctx, attrs, type='int'):
    vals = list(range(0, len(attrs)))

    if type is 'iter':
        vals = [[i] for i in vals]

    Foo  = Row(*attrs)
    foo1 = Foo(*vals)
    return ctx.createDataFrame(data=[foo1])

def mock_cassandra_read(ctx, table):
    return faux_dataframe(ctx, ['chipx', 'chipy', 'srb1'])

def mock_merlin_create(x, y, acquired, cfg):
    return [[[11, 22, 33, 44], (x, y)]]

def mock_timeseries_rdd(ctx, cids, acquired, cfg, name):
    rdd = ctx.parallelize([[[11, 22, 33, 44], (-999, 111)]])
    rdd.setName(name)
    return rdd


