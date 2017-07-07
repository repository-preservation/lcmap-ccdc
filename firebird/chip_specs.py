import requests

def get(query):
    """Queries aardvark and returns chip_specs
    :param query: full url query for aardvark
    :returns: sequence of chip specs
    :example:
    >>> chip_specs('http://host:port/v1/landsat/chip-specs?q=red AND sr')
    ('chip_spec_1', 'chip_spec_2', ...)
    """
    return tuple(requests.get(query).json())


def byubid(chip_specs):
    """Organizes chip_specs by ubid
    :param chip_specs: a sequence of chip specs
    :returns: a dict of chip_specs keyed by ubid
    """
    return {cs['ubid']: cs for cs in chip_specs}


def ubids(chip_specs):
    """Extract ubids from a sequence of chip_specs
    :param chip_specs: a sequence of chip_spec dicts
    :returns: a sequence of ubids
    """
    return tuple(cs['ubid'] for cs in chip_specs if 'ubid' in cs)
