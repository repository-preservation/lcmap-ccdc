import math
import numpy as np
from base64 import b64decode


def difference(point, interval):
    """
    Calculate difference between a point and 'prior' point on an interval.

    The value of this function can be used to answer the question,
    what do I subtract from a point to find the point of the nearest
    chip that contains it?

    Geospatial raster data geometry can be somewhat counter-inuitive
    because coordinates and pixel geometry are expressed with both
    positive and negative values.

    Along the x-axis, pixel size (and thus the interval) is a positive
    number (e.g. 30 * 100). Along the y-axis though, the pixel size and
    interval is a _negative_ value. Even though this may seem peculiar,
    using a negative value helps us avoid special cases for finding the
    nearest tile-point.

    :param point: a scalar value on the real number line
    :param interval: a scalar value describing regularly spaced points
                     on the real number line
    :returns: difference between a point and prior point on an interval.
    """
    return point % interval


def near(point, interval, offset):
    """
    Find nearest point given an interval and offset.

    The nearest point will be lesser than the point for a positive
    interval, and greater than the point for a negative interval as
    is required when finding an 'upper-left' point on a cartesian
    plane.

    This function is used to calculate the nearest points along the
    x- and y- axis.

    :param point: a scalar value on the real number line
    :param interval: a scalar value describing regularly spaced points
                     on the real number line
    :param offset: a scalar value used to shift point before and after
                  finding the 'preceding' interval.
    :returns: a number representing a point.
    """
    # original clojure code
    # (-> point (- offset) (/ interval) (Math/floor) (* interval) (+ offset)))
    return ((math.floor((point - offset) / interval)) * interval) + offset


def point_to_chip(x, y, x_interval, y_interval, x_offset, y_offset):
    """
    Find the nearest containing chip's point.

    The resulting `x` value will be less than or equal to the input
    while the resulting `y` value will be greater than or equal.

    For this function to work properly, intervals and offsets must be
    expressed in terms of projection coordinate system 'easting' and
    'northing' units.

    Along the x-axis, this works as expected. The interval is a multiple
    of pixel size and tile size (e.g. 30 * 100 = 3000). Along the y-axis
    the interval is negative because the pixel size is negative, as you
    move from the origin of a raster file, the y-axis value _decreases_.

    The offset value is used for grids that are not aligned with the
    origin of the projection coordinate system.

    :param x: longitudinal value
    :param y: latitude value
    :param x_interval:
    :param y_interval:
    :param x_offset:
    :param y_offset:
    :returns: a tuple containing x, y where x and y are the identifying
              coordinates of a chip.
    """
    return (near(x, x_interval, x_offset),
            near(y, y_interval, y_offset))


def snap(x, y, chip_spec):
    """
    Transform an arbitrary projection system coordinate (x,y) into the
    coordinate of the chip that contains it.

    This function only works when working with points on a cartesian plane,
    it cannot be used with other coordinate systems.

    :param x: x coordinate
    :param y: y coordinate
    :param chip_spec: parameters for a chip's grid system
    :returns: tuple of chip x & y
    """
    chip_x = chip_spec['chip_x']
    chip_y = chip_spec['chip_y']
    shift_x = chip_spec['shift_x']
    shift_y = chip_spec['shift_y']
    chip = point_to_chip(x, y, chip_x, chip_y, shift_x, shift_y)
    return int(chip[0]), int(chip[1])


def ids(ulx, uly, lrx, lry, chip_spec):
    """
    Returns all the chip ids that are needed to cover a supplied bounding box.

    :param ulx: upper left x coordinate
    :param uly: upper left y coordinate
    :param lrx: lower right x coordinate
    :param lry: lower right y coordinate
    :param chip_spec: dict containing chip_x, chip_y, shift_x, shift_y
    :returns: generator of tuples containing chip ids
    :example:
    # assumes chip sizes of 500 pixels
    >>> chip_ids = ids(1000, -1000, -500, 500, chip_spec)
    ((-1000, 500), (-500, 500), (-1000, -500), (-500, -500))
    """
    chip_width = chip_spec['chip_x']    # e.g.  3000 meters, width of chip
    chip_height = chip_spec['chip_y']   # e.g. -3000 meters, height of chip

    start_x, start_y = snap(ulx, uly, chip_spec)
    end_x, end_y = snap(lrx, lry, chip_spec)

    yield ((x, y) for x in np.arange(start_x, end_x + chip_width, chip_width)
                  for y in np.arange(start_y, end_y + chip_height, chip_height))
    #_output = []
    # for x in np.arange(start_x, end_x + chip_width, chip_width):
    #     for y in np.arange(start_y, end_y + chip_height, chip_height):
    #         _output.append((x, y))
    # return _output


def to_numpy(chip, chip_spec):
    """
    Removes base64 encoding of chip data and converts it to a numpy array
    :param chip: A chip
    :param chip_spec: Corresponding chip_spec
    :returns: A decoded chip with data as a shaped numpy array
    """
    shape = chip_spec['data_shape']
    dtype = chip_spec['data_type'].lower()
    cdata = b64decode(chip['data'])

    chip['data'] = np.frombuffer(cdata, dtype).reshape(*shape)
    return chip


def locations(startx, starty, chip_spec):
    """
    Computes locations for array elements that fall within the shape
    specified by chip_spec['data_shape'] using the startx and starty as
    the origin.  locations() does not snap() the startx and starty... this
    should be done prior to calling locations() if needed.
    :param startx: x coordinate (longitude) of upper left pixel of chip
    :param starty: y coordinate (latitude) of upper left pixel of chip
    :returns: A two (three) dimensional numpy array of [x, y] coordinates
    """
    cw = chip_spec['data_shape'][0] # 100
    ch = chip_spec['data_shape'][1] # 100

    pw = chip_spec['pixel_x'] # 30 meters
    ph = chip_spec['pixel_y'] # -30 meters

    # determine ends
    endx = startx + cw * pw
    endy = starty + ch * ph

    # build arrays of end - start / step shape
    # flatten into 1d, concatenate and reshape to fit chip
    x, y = np.mgrid[startx:endx:pw, starty:endy:ph]
    matrix = np.c_[x.ravel(), y.ravel()]
    return np.reshape(matrix, (cw, ch, 2))
