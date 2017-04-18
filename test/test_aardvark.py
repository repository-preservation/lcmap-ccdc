from firebird.aardvark import pyccd_tile_spec_queries, ubids

from hypothesis import given
import hypothesis.strategies as st
import urllib

@given(url=st.sampled_from(('http://localhost',
                            'https://localhost',
                            'http://localhost/',
                            'http://127.0.0.1')))
def test_pyccd_tile_spec_queries(url):
    def check(query):
        url = urllib.parse.urlparse(query)
        assert url.scheme
        assert url.netloc
    queries = pyccd_tile_spec_queries(url)
    [check(query) for query in queries.values()]


def test_ubids():
    data = ({'ubid': 'a/b/c'}, {'ubid': 'd/e/f'}, {'ubid': 'g'}, {'noubid': 'z'})
    good = filter(lambda f: 'ubid' in f, data)
    assert set(map(lambda u: u['ubid'], good)) == set(ubids(data))


def test_rodify():
    assert True is True
