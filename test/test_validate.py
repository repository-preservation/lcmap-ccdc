from firebird import validate
import pytest


def test_acquired():
    validate._acquired("1980-01-01/2015-12-31")

    with pytest.raises(Exception) as excinfo:
        validate._acquired("1980-01-01|2015-12-31")
        validate._acquired("1980-01-01015-12-31")
        validate._acquired("1990-01-01/1980-12-31")


def test_clip():
    validate._clip('FalSe')
    validate._clip(False)
    validate._clip(0)
    validate._clip('tRuE')
    validate._clip(True)
    validate._clip(1)

    with pytest.raises(Exception) as excinfo:
        validate._clip("If you aren't first you're last")


def test_bounds():
    validate._bounds(['123,456'])
    validate._bounds(['123,456', '555,444'])
    validate._bounds(['-123,-456', '555,-444', '776,212', '1,1', '889,23'])
    validate._bounds(['0,0', '0,0'])

    with pytest.raises(Exception) as excinfo:
        validate._bounds([])
        validate._bounds(['not a coordinate'])
        validate._bounds([True])
        validate._bounds(['111 222', ('665' '33333')])


def test_products():
    validate._products(['seglength'])
    validate._products(['seglength', 'changemag'])

    with pytest.raises(Exception) as excinfo:
        validate._products([])
        validate._products(['seglength', 'changemag', 'not-a-product'])


def test_product_dates():
    acquired = "1980-01-01/1990-01-01"
    validate._product_dates(acquired=acquired, product_dates=["1980-01-01"])

    with pytest.raises(Exception) as excinfo:
        validate._product_dates(acquired=acquired,
                                product_dates=["1979/01/01"])
