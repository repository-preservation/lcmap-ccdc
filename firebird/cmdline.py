from firebird import aardvark as a
from firebird import actions
import click as c
import firebird as fb


products = fb.available_products()


@c.group()
def fbcmd():
    pass


@fbcmd.command()
def products():
    return fb.available_products()


@fbcmd.command()
@c.option('--bounds', '-b', multiple=True)
@c.option('--product', '-p')
def count(bounds, product):
    """Returns count of number of results given bounds and a product.  Bounds
    will be minbox'd prior to calculation.
    :param bounds: Sequence of (x, y) sequences
    :param product: A firebird product
    :return: An integer count
    """
    pass


@fbcmd.command()
@c.option('--bounds', '-b')
def chipids(bounds):
    spec = a.chip_spec(d.chip_spec_queries(fb.SPECS_URL)['blues'])[0]
    return chip.ids(fb.minbox(bounds), spec)


@fbcmd.command()
@c.option('--bounds', '-b', multiple=True)
@c.option('--product', '-p')
def missing(bounds, product):
    """Returns ids that do not exist within the given bounds"""
    #  sc = fb.SparkContext()
    #  ss = sql.SparkSession(ss)
    #  ss.createDataFrame(fb.minbox(bounds).xys())
    #  ss.product.load()
    #  return bounds.subtract(product.load().xys())
    pass


@fbcmd.command()
@c.option('--directory', '-d')
@c.option('--iwds', '-i')
@c.option('--fresh', '-f', is_flag=True)
def train(from_iwds, acquired, bounds, products, product_dates):
    pass


@fbcmd.command()
@c.option('--acquired', '-a')
@c.option('--bounds', '-b', multiple=True)
@c.option('--clip', '-c', is_flag=True)
@c.option('--directory', '-d')
@c.option('--products', '-p', type=c.Choice(fb.available_products(),
          multiple=True)
@c.option('--product_dates', '-d', multiple=True)
def evaluate(acquired, bounds, clip, directory, product, product_dates):
    return actions.evaluate(acquired=acquired,
                            bounds=bounds,
                            clip=clip,
                            directory=directory,
                            products=products,
                            product_dates=product_dates)


@fbcmd.command()
@c.option('--acquired', '-a')
@c.option('--bounds', '-b', multiple=True)
@c.option('--clip', '-c', is_flag=True)
@c.option('--products', '-p', type=c.Choice(fb.available_products(),
          multiple=True)
@c.option('--product_dates', '-d', multiple=True)
def save(acquired, bounds, clip, product, product_dates):
    return actions.save(acquired=acquired,
                        bounds=bounds,
                        clip=clip,
                        products=products,
                        product_dates=product_dates)


if __name__ == "__main__":
    run()
