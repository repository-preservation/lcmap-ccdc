import click
from firebird import driver


@click.command()
@click.option('--prd',        prompt='production date')
@click.option('--acq',        prompt='acquired dates')
@click.option('--ulx',        prompt='upper left x coordinate')
@click.option('--uly',        prompt='upper left y coordinate')
@click.option('--lrx',        prompt='lower right x coordinate')
@click.option('--lry',        prompt='lower right y coordinate')
@click.option('--clip'        default=False)
@click.option('--ccd',        default=True)
@click.option('--lastchange', default=True)
@click.option('--changemag',  default=True)
@click.option('--changedate', default=True)
@click.option('--seglength',  default=True)
@click.option('--qa',         default=True)
@click.option('--par',        default=10000)
@click.option('--xr',         default=100)
@click.option('--yr',         default=100)
def run(prd, acq, ulx, uly, lrx, lry, lastchange, changemag, changedate, seglength, qa, par, xr, yr):
    driver.run(acquired=acq, ulx=ulx, uly=uly, lrx=lrx, lry=lry,
               prod_date=prd, lastchange=lastchange, changemag=changemag,
               changedate=changedate, seglength=seglength, qa=qa,
               parallelization=par, xrange=xr, yrange=yr)

if __name__ == "__main__":
    run()
