from firebird import products
from firebird import dtstr_to_ordinal as dto

def test_result_to_models():
    _res = {'result': '{"change_models": "foo bar"}'}
    assert products.result_to_models(_res) == "foo bar"


def test_lastchange():
    inmodels = [{'change_probability': 1, 'break_day': 999}]
    ord_date = 1000
    _res = products.lastchange(inmodels, ord_date)
    assert _res is 1


def test_changemag():
    pick_a_day = "1984-04-01"
    ord_date = dto(pick_a_day)
    inmodels = [{'change_probability': 1, 'break_day': ord_date}]
    _res = products.changemag(inmodels, ord_date)
    assert _res is 0.0


def test_changedate():
    pick_a_day = "1984-04-01"
    ord_date = dto(pick_a_day)
    inmodels = [{'change_probability': 1, 'break_day': ord_date}]
    _res = products.changedate(inmodels, ord_date)
    assert _res is 92
    

def test_seglength():
    assert 1 > 0


def test_qa():
    assert 1 > 0


def test_run():
    assert 1 > 0
