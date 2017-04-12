import click

@click.group
def cli():
    print("Starting firebird...")

@cli.command
def run(minx, miny, maxx, maxy, start_date, end_date, products, persist=False):
    pass

@cli.command
def products():
    pass
