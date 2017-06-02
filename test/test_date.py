from firebird import date as d


def test_to_ordinal():
    assert d.to_ordinal('1999-01-01') == 729755
    assert d.to_ordinal('1999/11/01') == 730059
    assert d.to_ordinal('2001') == 730638


def test_startdate():
    assert d.startdate('1980-01-01/1982-01-01') == '1980-01-01'


def test_enddate():
    assert d.enddate('1980-01-01/1982-01-01') == '1982-01-01'


def test_is_acquired():
    assert d.is_acquired('1980-01-01/1982-01-01') is True
    assert d.is_acquired('1980-01-011982-01-01') is False
