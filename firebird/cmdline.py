from cytoolz import first
from cytoolz import second
from firebird import actions
from firebird import transforms
from firebird import validate
from merlin import chip_specs
import click as c
import firebird as fb
import os
import pyspark


def list_products():
    """Available Firebird products that may be requested.

    Returns:
        list: Firebird products
    """

    return list(transforms.labels().keys())


def context_settings():
    """Normalized tokens for Click cli

    Returns:
        dict
    """

    return dict(token_normalize_func=lambda x: x.lower())


@c.group(context_settings=context_settings())
def cli():
    """Placeholder function to group Click commands"""

    pass


@cli.group()
def show():
    """Placeholder function to group Click commands"""

    pass


@show.command()
def products():
    """Click cli command for available Firebird products

    Returns:
        sequence: product names
    """

    c.echo(list_products())


@show.command()
def algorithms():
    """Click cli command for available Firebird algorithms (deprecated)

    Returns:
        sequence: algorithms
    """

    c.echo((os.path.exists('/algorithms') and
            os.path.isdir('/algorithms') and
            os.listdir('/algorithms')) or "No algorithms available.")


@show.command()
@c.option('--bounds', '-b', required=True)
def chip_coordinates(bounds):
    """Click cli command to display chip coordinates from a set of bounds

    Args:
        bounds (sequence): tuple of tuples.  ((x1,y1), (x1,y2),)

    Returns:
        sequence: chip ids
    """

    queries = d.chip_spec_queries(fb.SPECS_URL)
    spec = first(chip_specs.get(first(queries[first(queries)])))
    return chip.ids(fb.minbox(bounds), spec)


@cli.command()
@c.option('--acquired', '-a', required=True)
@c.option('--bounds', '-b', required=True, multiple=True)
@c.option('--products', '-p', required=True, multiple=True)
@c.option('--product_dates', '-d', required=True, multiple=True)
@c.option('--clip', '-c', is_flag=True, default=False)
def save(acquired, bounds, products, product_dates, clip):
    """Click cli command to generate and save Firebird products

    Args:
        acquired (str): iso8601 date range
        bounds (tuple): tuple of tuples. 1 to N.  ((x1,y1), (x1,y2),) ...
        products (tuple): Firebird products to generate and save
        product_dates (tuple): Product dates for change products
        clip (bool): Clip products to bounds or save all chips touching bounds

    Returns:
        dict: {product: dataframe}
    """

    # TODO: handle accepting tile ids

    validate.save(acquired=acquired,
                  bounds=bounds,
                  clip=clip,
                  products=products,
                  product_dates=product_dates)

    # convert bounds to numbers from string sequence.
    fbounds = map(lambda n: (float(n[0]),float(n[1])),
                  map(lambda x: x.split(','), bounds))

    spark_context = None
    try:
        spark_context = pyspark.SparkContext()

        results = list(actions.counts(
                           actions.save(acquired=acquired,
                                        bounds=list(fbounds),
                                        clip=clip,
                                        products=products,
                                        product_dates=product_dates,
                                        spark_context=spark_context)))
        print(results)
        return results
    finally:
        if spark_context is not None:
            spark_context.stop()


if __name__ == "__main__":
    cli()
