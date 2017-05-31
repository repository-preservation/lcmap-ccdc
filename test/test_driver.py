from firebird import driver
from test.mocks import chip as mc, aardvark as ma, driver as md
from test.mocks import sparkcon
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


@patch('firebird.chip.ids', mc.ids)
@patch('firebird.aardvark.chips', ma.chips)
@patch('firebird.aardvark.chip_specs', ma.chip_specs)
@patch('firebird.driver.pyccd_rdd', md.pyccd_rdd)
@patch('firebird.validation.acquired', lambda d: True)
@patch('firebird.validation.coords', lambda ulx, uly, lrx, lry: True)
@patch('firebird.validation.prod', lambda p: True)
def test_run():
    # mocking driver's pyccd_rdd func because we dont need to run ccd.detect on 10k pixels
    acq = "01-01-1969/12-31-1999"
    ulx = -100300
    uly = 200000
    lrx = -100000
    lry = 1999970
    prd = '1984-04-01'
    run_resp = driver.run(acq, ulx, uly, lrx, lry, prd, parallelization=1, sparkcontext=sparkcontext)
    assert run_resp is True


@patch('firebird.aardvark.chips', ma.chips)
@patch('firebird.aardvark.chip_specs', ma.chip_specs)
def test_detect():
    inputs = driver.pyccd_rdd('http://localhost', 'http://localhost',
                              -100200, 300400, '1980-01-01/2015-12-31')
    results = driver.detect(*inputs[0][0], inputs[0][1], *inputs[0][0])
    assert results['result_ok'] is True


@patch('firebird.aardvark.chips', ma.chips)
@patch('firebird.aardvark.chip_specs', ma.chip_specs)
def test_detect_ccd_exception():
    _rdd = driver.pyccd_rdd('http://localhost', 'http://localhost', -100200, 300400, '1980-01-01/2015-12-31')
    band_dict = _rdd[0][1]
    band_dict.pop('reds')
    results = driver.detect(111111, 222222, band_dict, 33333, 44444)
    assert results['result_ok'] is False


@patch('firebird.aardvark.chips', ma.chips)
@patch('firebird.aardvark.chip_specs', ma.chip_specs)
def test_pyccd_rdd():
    inputs = driver.pyccd_rdd('http://localhost', 'http://localhost',
                              -100200, 300400, '1980-01-01/2015-12-31')
    assert len(inputs) == 10000
    assert isinstance(inputs[0], tuple)
    assert isinstance(inputs[0][0], tuple)
    assert isinstance(inputs[0][1], dict)
