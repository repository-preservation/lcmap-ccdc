from firebird import products
from merlin import dates as d


def test_lastchange():
    assert products.lastchange([{'change_probability': 1, 'break_day': 999}], 1000) == 1


def test_changemag():
    ord_date = d.to_ordinal("1984-04-01")
    assert products.changemag([{'change_probability': 1, 'break_day': ord_date}], ord_date) == 0.0


def test_changedate():
    ord_date = d.to_ordinal("1984-04-01")
    assert products.changedate([{'change_probability': 1, 'break_day': ord_date}], ord_date) == 92


def test_seglength():
    assert products.seglength([{'start_day': d.to_ordinal("1986-04-01"),
                                'end_day': d.to_ordinal("1988-07-01")}],
                              d.to_ordinal("1984-04-01"),
                              bot=d.to_ordinal("1982-01-01")) == 821


def test_qa():
    assert products.curveqa([{'start_day': d.to_ordinal("1986-04-01"),
                              'end_day': d.to_ordinal("1988-07-01"),
                              'curve_qa': 999}],
                             d.to_ordinal('1987-01-01')) == 999
