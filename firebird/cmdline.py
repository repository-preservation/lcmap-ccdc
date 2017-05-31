import click as c
from firebird import driver

available_products = driver.products_graph().keys()


@c.group()
def fbcmd():
    pass


@fbcmd.command()
def products():
    return available_products


@fbcmd.command()
@c.option('--acquired', '-a')
@c.option('--bounds', '-b', multiple=True)
@c.option('--clip', '-c', is_flag=True)
@c.option('--products', '-p', type=c.Choice(available_products, multiple=True)
@c.option('--product_dates', '-d', multiple=True)
def run(acquired, bounds, product, product_dates, clip):
    return driver.run(acquired, bounds, products, product_dates, clip)


if __name__ == "__main__":
    run()
