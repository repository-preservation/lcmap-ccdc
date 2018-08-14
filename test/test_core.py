from ccdc import core

import test

@test.vcr.use_cassette(test.cassette)
def test_changedetection():
    result = core.changedetection(x=0, y=0, number=1, chunk_size=1)
    assert result == []

def test_classification():
    pass
