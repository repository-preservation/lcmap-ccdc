from functools import partial
from pyspark.sql import Row
from test import TEST_ROOT
from numpy import array
from numpy import int16, uint16

import json

acquired   = '1980-01-01/2017-01-01'
ard_schema = ['chipx', 'chipy', 'x', 'y', 'dates', 'blues', 'greens', 'reds', 'nirs', 
              'swir1s', 'swir2s', 'thermals', 'qas']
aux_schema = ['chipx', 'chipy', 'x', 'y', 'dates', 'dem', 'trends', 'aspect', 'posidex', 
              'slope', 'mpw']
dummy_list = [[5, 4, 3, 2], {"foo": 66}]

features_columns = ['blmag',  'grmag',  'remag',  'nimag',  's1mag',  's2mag',  'thmag',
                    'blrmse', 'grrmse', 'rermse', 'nirmse', 's1rmse', 's2rmse', 'thrmse',
                    'blcoef', 'grcoef', 'recoef', 'nicoef', 's1coef', 's2coef', 'thcoef',
                    'blint',  'grint',  'reint',  'niint',  's1int',  's2int',  'thint',
                    'dem',    'aspect', 'slope',  'mpw',    'posidex']

ccd_schema_names = ['chipx', 'chipy', 'x', 'y', 'sday', 'eday', 'bday', 'chprob', 'curqa',
                    'blmag', 'grmag', 'remag', 'nimag', 's1mag', 's2mag', 'thmag', 'blrmse',
                    'grrmse', 'rermse', 'nirmse', 's1rmse', 's2rmse', 'thrmse', 'blcoef', 'grcoef',
                    'recoef', 'nicoef', 's1coef', 's2coef', 'thcoef', 'blint', 'reint', 'niint',
                    's1int', 's2int', 'thint', 'dates', 'snprob', 'waprob', 'clprob', 'prmask', 'rfrawp']

ccd_format_keys = ['chipx', 'chipy', 'x', 'y', 'sday', 'eday', 'bday', 'chprob', 'curqa',
                    'blmag', 'grmag', 'remag', 'nimag', 's1mag', 's2mag', 'thmag', 'blrmse',
                    'grrmse', 'rermse', 'nirmse', 's1rmse', 's2rmse', 'thrmse', 'blcoef', 'grcoef',
                    'recoef', 'nicoef', 's1coef', 's2coef', 'thcoef', 'blint', 'reint', 'niint',
                    's1int', 's2int', 'thint', 'dates', 'snprob', 'waprob', 'clprob', 'prmask', 'grint']

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

def merge_lists(lists):
    output = []
    for i in lists:
        output.extend(i)
    output = set(output)
    return list(output)

merged_schema = merge_lists([ard_schema, aux_schema])

def mock_cassandra_read(ctx, table):
    return faux_dataframe(ctx, ['chipx', 'chipy', 'srb1'])

def mock_merlin_create(x, y, acquired, cfg):
    return [[[11, 22, 33, 44], (x, y)]]

def mock_timeseries_rdd(ctx, cids, acquired, cfg, name):
    rdd = ctx.parallelize([[[11, 22, 33, 44], (-999, 111)]])
    rdd.setName(name)
    return rdd


