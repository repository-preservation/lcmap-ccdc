import json

acquired   = '1980-01-01/2017-01-01'

ard_schema = ['chipx', 'chipy', 'x', 'y', 'dates', 'blues', 'greens', 'reds', 'nirs', 'swir1s', 'swir2s', 'thermals', 'qas']

aux_schema = ['chipx', 'chipy', 'x', 'y', 'dates', 'dem', 'trends', 'aspect', 'posidex', 'slope', 'mpw']

dummy_list = [[5, 4, 3, 2], {"foo": 66}]

def mock_merlin_create(x, y, acquired, cfg):
    return [[[11, 22, 33, 44], (x, y)]]

def mock_timeseries_rdd(ctx, cids, acquired, cfg, name):
    rdd = ctx.parallelize([[[11, 22, 33, 44], (-999, 111)]])
    rdd.setName(name)
    return rdd

tile_resp  = json.loads(open("test/data/tile_response.json").read())

