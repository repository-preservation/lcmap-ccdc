from firebird.aardvark import *

from hypothesis import given
import hypothesis.strategies as st
import urllib

@given(url=st.sampled_from(('http://localhost', 'https://localhost', 'http://127.0.0.1')))
def test_pyccd_tile_spec_queries(url):
    def check(query):
        url = urllib.parse.urlparse(query)
        assert url.scheme
        assert url.netloc
    queries = pyccd_tile_spec_queries(url)
    [check(query) for query in queries.values()]
