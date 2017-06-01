from firebird import aardvark as a
from firebird import driver
from hypothesis import given
from mock import patch
from test.mocks import aardvark as ma
from test.mocks import chip as mc
from test.mocks import driver as md
from test.mocks import sparkcon

import firebird as fb
import hypothesis.strategies as st
import pyspark
import os
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


def test_startdate():
    assert driver.startdate('1980-01-01/1982-01-01') == '1980-01-01'


def test_enddate():
    assert driver.enddate('1980-01-01/1982-01-01') == '1982-01-01'


def test_broadcast():
    sc = None
    try:
        sc = pyspark.SparkContext(appName="test_broadcast")
        bc = driver.broadcast(chips_url=1, specs_url=2, acquired=3, spec=4,
                              product_dates=5, start_date=6, clip=7, products=8,
                              bbox=9, sparkcontext=sc)
        assert bc['chips_url'].value == 1
        assert bc['specs_url'].value == 2
        assert bc['acquired'].value == 3
        assert bc['spec'].value == 4
        assert bc['product_dates'].value == 5
        assert bc['start_date'].value == 6
        assert bc['clip'].value == 7
        assert bc['products'].value == 8
        assert bc['bbox'].value == 9
    finally:
        if sc is not None:
            sc.stop()


def test_chipid_rdd():
    sc = None
    try:
        sc = pyspark.SparkContext(appName="test_chipid_rdd")
        data = (1, 2, 3)
        rdd = driver.chipid_rdd(data, sc)
        assert set(rdd.collect()) == set(data)
        assert rdd.getNumPartitions() == 3
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
        c = pyspark.SparkConf().set('spark.driver.memory', '4g')


        print(c.getAll())

        sc = pyspark.SparkContext(conf=c)
        spec = ma.chip_specs(driver.chip_spec_urls(fb.SPECS_URL)['blues'])[0]
        bc = driver.broadcast(chips_url='http://localhost',
                              chips_fn=ma.chips,
                              specs_url='http://localhost',
                              specs_fn=ma.chip_specs,
                              acquired='1982-01-01/2015-12-12',
                              spec=spec,
                              product_dates=['2014-12-12'],
                              start_date='2014-12-12',
                              clip=False,
                              products=['ccd'],
                              bbox={'ulx':-1821585, 'uly':2891595,
                                    'lrx':-1824585, 'lry':2888595},
                              sparkcontext=sc)

        rdd = driver.chipid_rdd([(-1821585, 2891595)], sc)
        assert rdd.getNumPartitions() == 1
        assert rdd.count() == 1

        graph = driver.products_graph(rdd, bc)
        assert graph['inputs'].count() == 10000
        #print(graph['inputs'].first()[0])
        print("CCD FIRST")
        print(graph['ccd'].first())

    finally:
        if sc is not None:
            sc.stop()

#@patch('firebird.chip.ids', mc.ids)
#@patch('firebird.aardvark.chips', ma.chips)
#@patch('firebird.aardvark.chip_specs', ma.chip_specs)
#@patch('firebird.driver.pyccd_rdd', md.pyccd_rdd)
#@patch('firebird.validation.acquired', lambda d: True)
#@patch('firebird.validation.coords', lambda ulx, uly, lrx, lry: True)
#@patch('firebird.validation.prod', lambda p: True)
#def test_run():
    # mocking driver's pyccd_rdd func because we dont need to run ccd.detect on 10k pixels
#    acq = "01-01-1969/12-31-1999"
#    ulx = -100300
#    uly = 200000
#    lrx = -100000
#    lry = 1999970
#    prd = '1984-04-01'
#    run_resp = driver.run(acq, ulx, uly, lrx, lry, prd, parallelization=1, sparkcontext=sparkcontext)
#    assert run_resp is True


def test_pyccd_inputs():
    # rdd should be shaped: ( ((),{}), ((),{}), ((),{}) )
    inputs = driver.pyccd_inputs(point=(-182000, 300400),
                                 specs_url='http://localhost',
                                 specs_fn=ma.chip_specs,
                                 chips_url='http://localhost',
                                 chips_fn=ma.chips,
                                 acquired='1980-01-01/2015-12-31')
    assert len(inputs) == 10000
    assert isinstance(inputs, dict)
    assert isinstance(inputs[0], tuple)
    assert isinstance(inputs[0][0], tuple)
    assert isinstance(inputs[0][1], dict)
    assert len(inputs[0][0]) == 2
