from firebird import actions as a
from firebird import functions as f
from functools import partial
from test.mocks import aardvark as ma
import firebird as fb
import os
import pyspark

def test_broadcast():
    sc = None
    try:
        sc = pyspark.SparkContext(appName="test_broadcast")
        sc.setLogLevel(fb.LOG_LEVEL)
        bc = a.broadcast({'a': 'a',
                          'true': True,
                          'list': [1, 2, 3],
                          'set': set([1, 2, 3]),
                          'tuple': tuple([1, 2, 3]),
                          'dict': dict({'a': 1}),
                          'none': None,
                          'num': 3}, sparkcontext=sc)

        assert bc['a'].value == 'a'
        assert bc['true'].value == True
        assert bc['list'].value == [1, 2, 3]
        assert bc['set'].value == {1, 2, 3}
        assert bc['tuple'].value == (1, 2, 3)
        assert bc['dict'].value == {'a': 1}
        assert bc['num'].value == 3
        assert bc['none'].value == None
    finally:
        if sc is not None:
            sc.stop()


def test_init():
    sc = None
    try:
        sc = pyspark.SparkContext(appName="test_init")
        sc.setLogLevel(fb.LOG_LEVEL)
        spec = ma.chip_specs(fb.chip_spec_queries(fb.SPECS_URL)['blues'])[0]
        acquired = '1982-01-01/2015-12-12'
        chip_ids = ((-1821585, 2891595),)
        clip_box = f.minbox(chip_ids)
        products = ['inputs', 'ccd', 'lastchange',
                    'changemag', 'seglength', 'curveqa']
        product_dates = ['2014-12-12']

        job, jobconf = a.init(acquired=acquired,
                              chip_ids=chip_ids,
                              products=products,
                              product_dates=product_dates,
                              sparkcontext=sc,
                              chips_fn=ma.chips,
                              specs_fn=ma.chip_specs,
                              clip_box=clip_box,
                              initial_partitions=2,
                              product_partitions=2,)

        assert jobconf['acquired'].value == '1982-01-01/2015-12-12'
        assert jobconf['chip_ids'].value == chip_ids
        assert jobconf['chips_fn'].value == ma.chips
        assert jobconf['chips_url'].value == fb.CHIPS_URL
        assert jobconf['clip_box'].value['ulx'] == clip_box['ulx']
        assert jobconf['clip_box'].value['uly'] == clip_box['uly']
        assert jobconf['clip_box'].value['lrx'] == clip_box['lrx']
        assert jobconf['clip_box'].value['lry'] == clip_box['lry']
        assert jobconf['products'].value == products
        assert jobconf['product_dates'].value == product_dates
        assert jobconf['reference_spec'].value is not None
        assert isinstance(jobconf['reference_spec'].value, dict)
        assert jobconf['specs_url'].value == fb.SPECS_URL
        assert jobconf['specs_fn'].value == ma.chip_specs
        assert isinstance(jobconf['initial_partitions'].value, int)
        assert isinstance(jobconf['product_partitions'].value, int)

        def check_count(p):
            assert p.count() == 1

        [check_count(job[p]) for p in products]
    finally:
        if sc is not None:
            sc.stop()


def test_save():
        spec = ma.chip_specs(fb.chip_spec_queries(fb.SPECS_URL)['blues'])[0]
        acquired = '1982-01-01/2015-12-12'
        chip_ids = ((-1821585, 2891595),)
        clip_box = f.minbox(chip_ids)
        products = ['inputs', 'ccd', 'lastchange',
                    'changemag', 'seglength', 'curveqa']
        product_dates = ['2014-12-12']

        results = a.save(acquired=acquired,
                         bounds=chip_ids,
                         products=products,
                         product_dates=product_dates,
                         clip=True,
                         chips_fn=ma.chips,
                         specs_fn=ma.chip_specs,
                         sparkcontext_fn=partial(pyspark.SparkContext,
                                                 appName="test_save"))
        for r in results:
            print("Save result:{}".format(r))
