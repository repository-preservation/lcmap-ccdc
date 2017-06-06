from collections import namedtuple
from firebird import driver
from firebird import rdds
from test.mocks import aardvark as ma
import firebird as fb
import pyspark


def test_result_to_models():
    ChangeModel = namedtuple('ChangeModel', ['start_day', 'end_day'])
    m = ChangeModel(start_day='1980-01-01', end_day='2017-06-05')
    result = rdds.result_to_models({'change_models': [m]})
    assert isinstance(result, list)
    assert 'start_day' in result[0]
    assert 'end_day' in result[0]

'''
def test_ccd():
    inputs = driver.pyccd_inputs(point=(-100200, 300400),
                                 specs_url='http://localhost',
                                 specs_fn=ma.chip_specs,
                                 chips_url='http://localhost',
                                 chips_fn=ma.chips,
                                 acquired='1980-01-01/2015-12-31')
    results = products.ccd((inputs[0][0], inputs[0][1]))
    assert type(results[0]) == tuple
    assert type(results[1]) == dict
    assert len(results[0]) == 3


def test_ccd_exception():
    inputs = driver.pyccd_inputs(point=(-100200, 300400),
                                 specs_url='http://localhost',
                                 specs_fn=ma.chip_specs,
                                 chips_url='http://localhost',
                                 chips_fn=ma.chips,
                                 acquired='1980-01-01/2015-12-31')
    band_dict = inputs[0][1]
    band_dict.pop('reds')
    try:
        results = products.ccd(((111111, 222222), band_dict))
    except Exception as e:
        assert e is not None
'''

def test_products():
    sc = None
    try:
        #c = pyspark.SparkConf()\
        #        .set('spark.driver.memory', '4g')\
        #        .set('spark.driver.host', fb.DRIVER_HOST)\
        #        .set('spark.driver.port', 0)\
        #        .set('spark.master', 'spark://local[*]')\
        #        .set('spark.rdd.compress', 'True')\
        #        .set('spark.serializer.objectStreamReset', '100')\
        #        .set('spark.submit.deployMode', 'client')\
        #        .set('spark.app.name', 'test_products_graph')\
        #        .set('spark.app.id', 'local-1496335206639')\
        #        .set('spark.executor.id', 'driver')
        # c = pyspark.SparkConf().set('spark.driver.memory', '1g')

        # we just want to test 1 point only.
        bounds = ((-1821585, 2891595),)
        queries = driver.chip_spec_queries(fb.SPECS_URL)
        spec = ma.chip_specs(queries['blues'])[0]

        sc = pyspark.SparkContext()

        bc = fb.broadcast({'acquired': '1982-01-01/2015-12-12',
                           'chip_ids': bounds,
                           'initial_partitions': 1,
                           'chips_fn': ma.chips,
                           'chip_spec_queries': queries,
                           'chips_url': 'http://localhost',
                           'clip': True,
                           'clip_box': fb.minbox(bounds),
                           'products': ['ccd'],
                           'product_dates': ['2014-12-12'],
                           'product_partitions': 1,
                           # should be able to pull this from the
                           # specs_fn and specs_url but this lets us
                           # do it once without beating aardvark up.
                           'reference_spec': spec,
                           'specs_url': 'http://localhost',
                           'specs_fn': ma.chip_specs},
                          sparkcontext=sc)
        graph = rdds.products(bc, sc)

        assert graph['inputs'].getNumPartitions() == 1
        assert graph['inputs'].count() == 1
        assert graph['ccd'].getNumPartitions() == 1
        assert graph['ccd'].count() == 1
    finally:
        if sc is not None:
            sc.stop()
