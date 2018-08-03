"""cli.py is the command line interface for CCDC.  

"""

from ccdc import core
import click


def context_settings():
    """Normalized tokens for Click cli

    Returns:
        dict
    """

    return dict(token_normalize_func=lambda x: x.lower())


@click.group(context_settings=context_settings())
def entrypoint():
    """Placeholder function to group Click commands"""
    pass


@entrypoint.command()
@click.option('--x',        '-x', required=True)
@click.option('--y',        '-y', required=True)
@click.option('--acquired', '-a', required=False, default=core.acquired())
@click.option('--number',   '-n', required=False, default=2500)
def changedetection(x, y, acquired, number):
    """Run change detection for a tile and save results to Cassandra.
    
    Args:
        x        (int): tile x coordinate
        y        (int): tile y coordinate
        acquired (str): ISO8601 date range
        number   (int): Number of chips to run change detection on.  Testing only.

    Returns:
        count of saved segments 
    """

    return core.changedetection(x=x,
                                y=y,
                                acquired=acquired,
                                number=number)

                        
@entrypoint.command()
@click.option('--x', '-x', required=True)
@click.option('--y', '-y', required=True)
@click.option('--msday', '-s', required=True)
@click.option('--meday', '-e', required=True)
@click.option('--acquired', '-a', required=False, default=acquired())
def classification(x, y, msday, meday, acquired): 
    """
    Classify a tile.

    Args:
        acquired (str): ISO8601 date range       
        x        (int): x coordinate in tile
        y        (int): y coordinate in tile
        msday    (int): ordinal day, beginning of training period
        meday    (int): ordinal day, end of training period
        acquired (str): date range of change segments to classify
    """

    return core.classification(x=x,
                               y=y,
                               msday=msday,
                               meday=meday,
                               acquired=acquired)

            
if __name__ == '__main__':
    entrypoint()
