from firebird import actions
from firebird import chip_specs
from firebird import files
from firebird import transforms
from firebird import validate
import click as c
import firebird as fb
import os


def list_products():
    return list(transforms.labels().keys())


def context_settings():
    return dict(token_normalize_func=lambda x: x.lower())


@c.group(context_settings=context_settings())
def cli():
    pass


@cli.group()
def show():
    pass


@show.command()
def products():
    """Returns a sequence of the products that may be requested from firebird
    :return: Sequence of product names
    """
    c.echo(list_products())


@show.command()
def algorithms():
    c.echo((os.path.exists('/algorithms') and
            os.path.isdir('/algorithms') and
            os.listdir('/algorithms')) or "No algorithms available.")


@cli.command()
@c.option('--bounds', '-b', required=True)
@c.option('--product', '-p', required=True)
def count(bounds, product):
    """Returns count of number of results given bounds and a product.  Bounds
    will be minbox'd prior to calculation.
    :param bounds: Sequence of (x, y) sequences
    :param product: A firebird product
    :return: An integer count
    """
    pass


@show.command()
@c.option('--bounds', '-b', required=True)
def chipids(bounds):
    spec = chip_specs.get(d.chip_spec_queries(fb.SPECS_URL)['blues'])[0]
    return chip.ids(fb.minbox(bounds), spec)


@cli.command()
@c.option('--acquired', '-a', required=True)
@c.option('--bounds', '-b', required=True, multiple=True)
@c.option('--products', '-p', required=True, multiple=True)
@c.option('--product_dates', '-d', required=True, multiple=True)
@c.option('--clip', '-c', is_flag=True)
def save(acquired, bounds, products, product_dates, clip):

    validate.save(acquired=acquired,
                  bounds=bounds,
                  clip=clip,
                  products=products,
                  product_dates=product_dates)

    # TODO: convert bounds to numbers from string sequence.

    results = actions.save(acquired=acquired,
                           bounds=bounds,
                           clip=clip,
                           products=products,
                           product_dates=product_dates)
    return list(results)


@cli.command()
@c.option('--name', '-n', required=True)
def test(name):
    c.echo(name)


if __name__ == "__main__":
    cli()
