from firebird import aardvark as a
from firebird import chip
from firebird import driver
from hypothesis import given
from test.mocks import aardvark as ma

import firebird as fb
import hypothesis.strategies as st
import os
import pyspark
import socket
import urllib


@given(url=st.sampled_from(('http://localhost',
                            'https://localhost',
                            'http://localhost/',
                            'http://127.0.0.1')))
def test_chip_spec_urls(url):
    def check(query):
        url = urllib.parse.urlparse(query)
        assert url.scheme
        assert url.netloc
    urls = driver.chip_spec_urls(url)
    [check(url) for url in urls.values()]


def test_csort():
    inputs = list()
    inputs.append({'acquired': '2015-04-01'})
    inputs.append({'acquired': '2017-04-01'})
    inputs.append({'acquired': '2017-01-01'})
    inputs.append({'acquired': '2016-04-01'})
    results = driver.csort(inputs)
    assert(results[0]['acquired'] > results[1]['acquired'] >
           results[2]['acquired'] > results[3]['acquired'])


def test_to_rod():
    assert 1 > 0


def test_to_pyccd():
    assert 1 > 0


def test_pyccd_dates():
    assert 1 > 0


def test_pyccd_inputs():
    # data should be shaped: ( ((),{}), ((),{}), ((),{}) )
    inputs = driver.pyccd_inputs(point=(-182000, 300400),
                                 specs_url='http://localhost',
                                 specs_fn=ma.chip_specs,
                                 chips_url='http://localhost',
                                 chips_fn=ma.chips,
                                 acquired='1980-01-01/2015-12-31')
    assert len(inputs) == 10000
    assert isinstance(inputs, tuple)
    assert isinstance(inputs[0], tuple)
    assert isinstance(inputs[0][0], tuple)
    assert isinstance(inputs[0][1], dict)
    assert len(inputs[0][0]) == 2


def test_broadcast():
    sc = None
    try:
        sc = pyspark.SparkContext(appName="test_broadcast")
        bc = driver.broadcast({'a': 'a',
                               'true': True,
                               'list': [1, 2, 3],
                               'set': set([1, 2, 3]),
                               'tuple': tuple([1, 2, 3]),
                               'dict': dict({'a': 1}),
                               'num': 3}, sparkcontext=sc)

        assert bc['a'].value == 'a'
        assert bc['true'].value == True
        assert bc['list'].value == [1, 2, 3]
        assert bc['set'].value == {1, 2, 3}
        assert bc['tuple'].value == (1, 2, 3)
        assert bc['dict'].value == {'a': 1}
        assert bc['num'].value == 3
    finally:
        if sc is not None:
            sc.stop()


def test_products_graph():
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

        spec = ma.chip_specs(driver.chip_spec_urls(fb.SPECS_URL)['blues'])[0]

        sc = pyspark.SparkContext()

        bc = driver.broadcast({'acquired': '1982-01-01/2015-12-12',
                               'chip_ids': bounds,
                               'initial_partitions': 1,
                               'chips_fn': ma.chips,
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
        graph = driver.products_graph(bc, sc)

        assert graph['inputs'].getNumPartitions() == 1
        assert graph['inputs'].count() == 1
        assert graph['ccd'].getNumPartitions() == 1
        assert graph['ccd'].count() == 1
    finally:
        if sc is not None:
            sc.stop()


def test_init():
    sc = None
    try:
        acquired = '1982-01-01/2015-12-12'
        bounds = ((-1821585, 2891595),)
        products = ['inputs', 'ccd', 'lastchange',
                    'changemag', 'seglength', 'curveqa']
        product_dates = ['2014-12-12']
        spec = ma.chip_specs(driver.chip_spec_urls(fb.SPECS_URL)['blues'])[0]

        job = driver.init(acquired=acquired,
                          bounds=bounds,
                          products=products,
                          product_dates=product_dates,
                          clip=True,
                          chips_fn=ma.chips,
                          initial_partitions=2,
                          product_partitions=2,
                          specs_fn=ma.chip_specs,
                          sparkcontext=pyspark.SparkContext)

        jc = job['jobconf']
        sc = job['sparkcontext']

        assert jc['acquired'].value == '1982-01-01/2015-12-12'
        assert jc['chip_ids'].value == chip.ids(ulx=fb.minbox(bounds)['ulx'],
                                                uly=fb.minbox(bounds)['uly'],
                                                lrx=fb.minbox(bounds)['lrx'],
                                                lry=fb.minbox(bounds)['lry'],
                                                chip_spec=spec)
        assert jc['chips_fn'].value == ma.chips
        assert jc['chips_url'].value == fb.CHIPS_URL
        assert jc['clip'].value == True
        assert jc['clip_box'].value == fb.minbox(bounds)
        assert jc['products'].value == products
        assert jc['product_dates'].value == product_dates
        assert jc['reference_spec'].value is not None
        assert isinstance(jc['reference_spec'].value, dict)
        assert jc['specs_url'].value == fb.SPECS_URL
        assert jc['specs_fn'].value == ma.chip_specs
        assert isinstance(jc['initial_partitions'].value, int)
        assert isinstance(jc['product_partitions'].value, int)

        assert isinstance(job['sparkcontext'].startTime, int)

        def check_count(p):
            assert p.count() == 1

        [check_count(job['products'][p]) for p in products]
    finally:
        if sc is not None:
            sc.stop()


def test_save():
    pass
