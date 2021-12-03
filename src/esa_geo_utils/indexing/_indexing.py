from typing import Union, Sequence
from itertools import product
from shapely.wkb import loads
from shapely.geometry import (
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
)

# Note no I
LETTERS = "ABCDEFGHJKLMNOPQRSTUVWXYZ"
NUMBERS_LENGTH = {1: 5, 10: 4, 100: 3, 1_000: 2, 10_000: 1, 100_000: 0}
RESOLUTION = (1, 10, 100, 1_000, 10_000, 100_000)


def _coords_to_bng(
    eastings: Union[int, float],
    northings: Union[int, float],
    resolution: int
) -> str:
    # convert coordinates to string, padding if necessary
    eastings_string = str(int(eastings)).rjust(6, "0")
    northings_string = str(int(northings)).rjust(6, "0")

    # split the strings (index from end to handle Shetlands 7 digit northings)
    eastings_quotient = int(eastings_string[:-5])
    eastings_remainder = eastings_string[-5:]
    northings_quotient = int(northings_string[:-5])
    northings_remainder = northings_string[-5:]

    # translate those into numeric equivalents of the grid letters
    first_letter_index = (19 - northings_quotient) - (19 - northings_quotient) % 5 \
        + (eastings_quotient + 10) // 5
    second_letter_index = (19 - northings_quotient) * 5 % 25 + eastings_quotient % 5
    # Lookup letters
    letters = LETTERS[first_letter_index] + LETTERS[second_letter_index]
    # Look up length
    length = NUMBERS_LENGTH[resolution]

    # Concatenate and return
    return letters + eastings_remainder[:length] + northings_remainder[:length]


def _bng_polygon_bounding_box(
    geometry: Polygon,
    resolution: int
) -> Sequence[str]:
    """Get bng references that cover a polygon's bounding box."""
    x1, y1, x2, y2 = geometry.bounds

    # Floor lower bounds
    x1 = int(x1 // resolution * resolution)
    y1 = int(y1 // resolution * resolution)
    # Ceil upper bounds
    x2 = int(-1 * x2 // resolution * resolution * -1)
    y2 = int(-1 * y2 // resolution * resolution * -1)

    # Check if upper bounds fall with the same grid as upper
    if x2 - x1 <= resolution:
        if y2 - y1 <= resolution:
            return [_coords_to_bng(x1, y1, resolution)]

    # Use range to calculate the axes
    eastings_axis = range(x1, x2, resolution)
    northings_axis = range(y1, y2, resolution)

    # Use product to generate the collection of coordinates
    grid_coordinates = product(eastings_axis, northings_axis)

    # Return bng strings
    return [_coords_to_bng(eastings, northings, resolution)
            for eastings, northings in grid_coordinates]


def _bng_multipolygon_bounding_box(
    geometry: MultiPolygon,
    resolution: int
) -> Sequence[str]:
    """Get bng references that cover a multipolygon's bounding boxes."""
    return (list(set(
        [bng for geom in geometry.geoms
         for bng in _bng_polygon_bounding_box(geom, resolution)])))


def _bng_multipoint(
    geometry: MultiPoint,
    resolution: int
) -> Sequence[str]:
    """Get bng references for multipoint geometries."""
    return (list(set(
        [bng for geom in geometry.geoms
         for bng in _coords_to_bng(geom.x, geom.y, resolution)])))


def apply_bng_index(wkb: bytearray, resolution: int, how: str = None) -> Sequence[str]:
    if resolution not in RESOLUTION:
        raise ValueError(f"resolution must be one of: {RESOLUTION}")

    geometry = loads(bytes(wkb))

    if isinstance(geometry, Point):
        return [_coords_to_bng(geometry.x, geometry.y, resolution)]
    elif isinstance(geometry, MultiPoint):
        return _bng_multipoint(geometry, resolution)
    elif isinstance(geometry, Polygon):
        if (not how) or (how == 'bounding box'):
            return _bng_polygon_bounding_box(geometry, resolution)
    elif isinstance(geometry, MultiPolygon):
        if (not how) or (how == 'bounding box'):
            return _bng_multipolygon_bounding_box(geometry, resolution)
