from firebird import driver
from firebird.mocks import aardvark as ma
from hypothesis import given
from mock import patch
import hypothesis.strategies as st
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
def test_pyccd_rdd():
    _rdd = driver.pyccd_rdd('http://localhost', 'http://localhost', -100200, 300400, '1980-01-01/2015-12-31')
    # wonkiness to deal with the generator business
    _g = [[z for z in i] for i in _rdd][0]
    r1 = _g[0][0]
    assert len(_g[0]) == 10000
    assert isinstance(r1, tuple)
    assert isinstance(r1[0], tuple)
    assert isinstance(r1[1], dict)

