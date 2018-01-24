from cytoolz import first


def refspec(specs):
    """Returns a reference spec from a dict of specs

    Args:
        specs (dict): A dict of {"key": [spec1, spec2],}

    Returns:
        dict: A spec
    """
    return first(specs[first(specs)])    
