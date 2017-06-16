"""Functions for working with files in python"""
import os
from firebird import logger


def read(path):
    with open(path, 'r+') as handle:
        return handle.read()


def readb(path):
    with open(path, 'rb+') as handle:
        return handle.read()


def readlines(path):
    with open(path, 'r+') as handle:
        return handle.readlines()


def readlinesb(path):
    with open(path, 'rb+') as handle:
        return handle.readlines()


def write(path, data):
    with open(path, 'w+') as handle:
        return handle.write(data)


def writeb(path, data):
    with open(path, 'wb+') as handle:
        return handle.write(data)


def append(path, data):
    with open(path, 'a+') as handle:
        return handle.write(data)


def appendb(path, data):
    with open(path, 'ab+') as handle:
        return handle.write(data)


def delete(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.error("Exception deleting file:{}".format(path))
        logger.error(e)
        return False
    return True


def exists(path):
    return os.path.exists(path) and os.path.isfile(path)


def mkdirs(filename):
    """Ensures the set of directories exist for the supplied filename.
    :param filename: Full path to where the file should live
    :returns: Full file path if the directories were created or None
    """
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        return filename
    except:
        return None
