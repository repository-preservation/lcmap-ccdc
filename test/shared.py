from pyspark.sql import Row

import json

acquired   = '1980-01-01/2017-01-01'

ard_schema = ['chipx', 'chipy', 'x', 'y', 'dates', 'blues', 'greens', 'reds', 'nirs', 'swir1s', 'swir2s', 'thermals', 'qas']

aux_schema = ['chipx', 'chipy', 'x', 'y', 'dates', 'dem', 'trends', 'aspect', 'posidex', 'slope', 'mpw']

dummy_list = [[5, 4, 3, 2], {"foo": 66}]

def faux_dataframe(ctx, attrs, type='int'):
    vals = list(range(0, len(attrs)))

    if type is 'iter':
        vals = [[i] for i in vals]

    Foo  = Row(*attrs)
    foo1 = Foo(*vals)
    return ctx.createDataFrame(data=[foo1])

features_columns = ['blmag',  'grmag',  'remag',  'nimag',  's1mag',  's2mag',  'thmag',
                    'blrmse', 'grrmse', 'rermse', 'nirmse', 's1rmse', 's2rmse', 'thrmse',
                    'blcoef', 'grcoef', 'recoef', 'nicoef', 's1coef', 's2coef', 'thcoef',
                    'blint',  'grint',  'reint',  'niint',  's1int',  's2int',  'thint',
                    'dem',    'aspect', 'slope',  'mpw',    'posidex']

def merge_lists(lists):
    output = []
    for i in lists:
        output.extend(i)
    output = set(output)
    return list(output)

merged_schema = merge_lists([ard_schema, aux_schema])

def mock_merlin_create(x, y, acquired, cfg):
    return [[[11, 22, 33, 44], (x, y)]]

def mock_timeseries_rdd(ctx, cids, acquired, cfg, name):
    rdd = ctx.parallelize([[[11, 22, 33, 44], (-999, 111)]])
    rdd.setName(name)
    return rdd

tile_resp  = json.loads(open("test/data/tile_response.json").read())

