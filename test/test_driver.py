from firebird import driver as driver
from hypothesis import given
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

def test_pyccd_rdd():
    assert 1 > 0
