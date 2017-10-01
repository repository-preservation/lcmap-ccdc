from firebird import cmdline
from click.testing import CliRunner

def test_save():
    # acquired, bounds, products, product_dates, clip
    acquired = '-a, 1980-01-01/2017-09-21'
    bounds = '-b, -1821585,2891595'
    products = '-p, inputs, -p, ccd, -p, lastchange, -p, changemag, -p, changedate, -p, seglength, -p, curveqa'
    product_dates = '-d, 2014'
    clip = True

    runner = CliRunner()
    result = runner.invoke(cmdline.save, [acquired, bounds, products, product_dates, clip])
    #assert 1 < 0
    print(result)
    #assert all([p in result for p in products])
