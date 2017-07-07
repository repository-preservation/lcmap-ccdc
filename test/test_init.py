from hypothesis import given
import firebird as fb
import hypothesis.strategies as st
import urllib


@given(url=st.sampled_from(('http://localhost',
                            'https://localhost',
                            'http://localhost/',
                            'http://127.0.0.1')))
def test_chip_spec_queries(url):
    def check(query):
        url = urllib.parse.urlparse(query)
        assert url.scheme
        assert url.netloc
    urls = fb.chip_spec_queries(url)
    [check(url) for url in urls.values()]
