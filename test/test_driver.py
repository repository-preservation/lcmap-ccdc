from firebird import driver
from test.mocks import aardvark as ma
from hypothesis import given
from mock import patch
import hypothesis.strategies as st
import os
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


@patch('firebird.aardvark.chips', ma.chips)
@patch('firebird.aardvark.chip_specs', ma.chip_specs)
def test_detect():
    inputs = driver.pyccd_rdd('http://localhost', 'http://localhost',
                              -100200, 300400, '1980-01-01/2015-12-31')
    results = driver.detect(123, 456, inputs[0][1], 123, 456)
    assert results['result_ok'] is True


@patch('firebird.aardvark.chips', ma.chips)
@patch('firebird.aardvark.chip_specs', ma.chip_specs)
def test_pyccd_rdd():
    inputs = driver.pyccd_rdd('http://localhost', 'http://localhost',
                              -100200, 300400, '1980-01-01/2015-12-31')
    assert len(inputs) == 10000
    assert isinstance(inputs[0], tuple)
    assert isinstance(inputs[0][0], tuple)
    assert isinstance(inputs[0][1], dict)
