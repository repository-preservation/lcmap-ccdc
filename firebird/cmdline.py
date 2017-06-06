import click as c
from firebird import driver

products = driver.available_products()


@c.group()
def fbcmd():
    pass


@fbcmd.command()
def products():
    return products


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
@c.option('--iwds', '-i', is_flag=True)
@c.option('--products', '-p', type=c.Choice(products, multiple=True)
@c.option('--product_dates', '-d', multiple=True)
def save(acquired, bounds, clip, directory, iwds, product, product_dates):
    return driver.save(acquired=acquired,
                       bounds=bounds,
                       clip=clip,
                       directory=directory,
                       iwds=iwds,
                       products=products,
                       product_dates=product_dates)


if __name__ == "__main__":
    run()
