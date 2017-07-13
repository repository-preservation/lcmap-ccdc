from firebird import actions
from firebird import chip_specs
from firebird import files
from firebird import transforms
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
@c.argument('--bounds', '-b')
@c.argument('--product', '-p')
def count(bounds, product):
    """Returns count of number of results given bounds and a product.  Bounds
    will be minbox'd prior to calculation.
    :param bounds: Sequence of (x, y) sequences
    :param product: A firebird product
    :return: An integer count
    """
    pass


@show.command()
@c.argument('--bounds', '-b')
def chipids(bounds):
    spec = chip_specs.get(d.chip_spec_queries(fb.SPECS_URL)['blues'])[0]
    return chip.ids(fb.minbox(bounds), spec)


@cli.command()
@c.option('--acquired', '-a')
@c.option('--bounds', '-b', multiple=True)
@c.option('--clip', '-c', is_flag=True)
@c.option('--products', '-p', type=c.Choice(list_products()),
          multiple=True)
@c.option('--product_dates', '-d', multiple=True)
def save(acquired, bounds, clip, product, product_dates):
    return actions.save(acquired=acquired,
                        bounds=bounds,
                        clip=clip,
                        products=products(),
                        product_dates=product_dates)


if __name__ == "__main__":
    cli()
