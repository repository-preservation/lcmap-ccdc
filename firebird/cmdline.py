from firebird import actions
from firebird import chip_specs
from firebird import transforms
import click as c
import firebird as fb


@c.group()
def fbcmd():
    pass


@fbcmd.command()
def products():
    """Returns a sequence of the products that may be requested from firebird
    :return: Sequence of product names
    """
    return transforms.labels().keys()


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
    spec = chip_specs.get(d.chip_spec_queries(fb.SPECS_URL)['blues'])[0]
    return chip.ids(fb.minbox(bounds), spec)


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
                        products=products(),
                        product_dates=product_dates)


if __name__ == "__main__":
    run()
