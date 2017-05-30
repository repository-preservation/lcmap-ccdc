from firebird import products
from firebird import dtstr_to_ordinal as dto
from test.shared import simplified_detect_results


def test_result_to_models():
    assert products.result_to_models({'result': '{"change_models": "foo bar"}'}) == "foo bar"


def test_lastchange():
    assert products.lastchange([{'change_probability': 1, 'break_day': 999}], 1000) == 1


def test_changemag():
    ord_date = dto("1984-04-01")
    assert products.changemag([{'change_probability': 1, 'break_day': ord_date}], ord_date) == 0.0


def test_changedate():
    ord_date = dto("1984-04-01")
    assert products.changedate([{'change_probability': 1, 'break_day': ord_date}], ord_date) == 92
    

def test_seglength():
    assert products.seglength([{'start_day': dto("1986-04-01"),
                                'end_day': dto("1988-07-01")}], dto("1984-04-01")) == 821


def test_qa():
    assert products.qa([{'start_day': dto("1986-04-01"), 'end_day': dto("1988-07-01"),
                         'qa': 999}], dto('1987-01-01')) == 999


def test_run():
    assert products.run('all', simplified_detect_results, dto("1980-01-01")) is True

