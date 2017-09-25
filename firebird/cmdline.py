from cytoolz import first
from cytoolz import second
from firebird import actions
from firebird import transforms
from firebird import validate
from merlin import chip_specs
from merlin import files
import click as c
import firebird as fb
import os
import pyspark


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


@show.command()
@c.option('--bounds', '-b', required=True)
def chip_coordinates(bounds):
    #spec = chip_specs.get(d.chip_spec_queries(fb.SPECS_URL)['blues'])[0]
    spec = first(chip_specs.get(first(d.chip_spec_queries(fb.SPECS_URL))))
    return chip.ids(fb.minbox(bounds), spec)


@cli.command()
@c.option('--acquired', '-a', required=True)
@c.option('--bounds', '-b', required=True, multiple=True)
@c.option('--products', '-p', required=True, multiple=True)
@c.option('--product_dates', '-d', required=True, multiple=True)
@c.option('--clip', '-c', is_flag=True, default=False)
def save(acquired, bounds, products, product_dates, clip):
    # TODO: handle accepting tile ids

    validate.save(acquired=acquired,
                  bounds=bounds,
                  clip=clip,
                  products=products,
                  product_dates=product_dates)

    # convert bounds to numbers from string sequence.
    fbounds = map(lambda n: (float(n[0]),float(n[1])),
                  map(lambda x: x.split(','), bounds))

    results = list(actions.counts(
                       actions.save(acquired=acquired,
                                    bounds=list(fbounds),
                                    clip=clip,
                                    products=products,
                                    product_dates=product_dates,
                                    spark_context=pyspark.SparkContext())))
    print(results)
    return results

if __name__ == "__main__":
    cli()
