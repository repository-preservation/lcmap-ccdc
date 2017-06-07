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
def test_chip_spec_queries(url):
    def check(query):
        url = urllib.parse.urlparse(query)
        assert url.scheme
        assert url.netlocs
    urls = driver.chip_spec_queries(url)
    [check(url) for url in urls.values()]


def test_init():
    sc = None
    try:
        acquired = '1982-01-01/2015-12-12'
        chip_ids = ((-1821585, 2891595),)
        clip_box = chip.ids(fb.minbox(chip_ids))
        products = ['inputs', 'ccd', 'lastchange',
                    'changemag', 'seglength', 'curveqa']
        product_dates = ['2014-12-12']
        spec = ma.chip_specs(driver.chip_spec_queries(fb.SPECS_URL)['blues'])[0]

        job = driver.init(acquired=acquired,
                          products=products,
                          product_dates=product_dates,
                          clip_box=clip_box,
                          chips_fn=ma.chips,
                          chip_ids=chip_ids,
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
