from firebird import actions as a
from firebird import functions as f
from test.mocks import aardvark as ma
import firebird as fb
import pyspark

def test_broadcast():
    sc = None
    try:
        sc = pyspark.SparkContext(appName="test_broadcast")
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
        sc = pyspark.SparkContext(appName="test_driver")
        spec = ma.chip_specs(fb.chip_spec_queries(fb.SPECS_URL)['blues'])[0]
        acquired = '1982-01-01/2015-12-12'
        chip_ids = ((-1821585, 2891595),)
        clip_box = f.minbox(chip_ids)
        products = ['inputs', 'ccd', 'lastchange',
                    'changemag', 'seglength', 'curveqa']
        product_dates = ['2014-12-12']

        job = a.init(acquired=acquired,
                     chip_ids=chip_ids,
                     products=products,
                     product_dates=product_dates,
                     sparkcontext=sc,
                     chips_fn=ma.chips,
                     specs_fn=ma.chip_specs,
                     clip_box=clip_box,
                     initial_partitions=2,
                     product_partitions=2,
                     )

        jc = job['jobconf']

        assert jc['acquired'].value == '1982-01-01/2015-12-12'
        assert jc['chip_ids'].value == chip_ids
        assert jc['chips_fn'].value == ma.chips
        assert jc['chips_url'].value == fb.CHIPS_URL
        assert jc['clip_box'].value['ulx'] == clip_box['ulx']
        assert jc['clip_box'].value['uly'] == clip_box['uly']
        assert jc['clip_box'].value['lrx'] == clip_box['lrx']
        assert jc['clip_box'].value['lry'] == clip_box['lry']
        assert jc['products'].value == products
        assert jc['product_dates'].value == product_dates
        assert jc['reference_spec'].value is not None
        assert isinstance(jc['reference_spec'].value, dict)
        assert jc['specs_url'].value == fb.SPECS_URL
        assert jc['specs_fn'].value == ma.chip_specs
        assert isinstance(jc['initial_partitions'].value, int)
        assert isinstance(jc['product_partitions'].value, int)

        def check_count(p):
            assert p.count() == 1

        [check_count(job['products'][p]) for p in products]
    finally:
        if sc is not None:
            sc.stop()
