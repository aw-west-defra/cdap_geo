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

# CONSTANTS
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


def _bng_geom_bounding_box(
    geometry: Union[LineString, Polygon],
    resolution: int,
    pad: int,
) -> Sequence[str]:
    """Get bng references that cover a geometry's bounding box."""
    x1, y1, x2, y2 = geometry.bounds
    # Arbitrary pad to ensure bounding box not on vertex/edge
    x1 -= pad
    y1 -= pad
    x2 += pad
    y2 += pad

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


def _bng_multigeom_bounding_box(
    geometry: Union[MultiLineString, MultiPolygon],
    resolution: int,
    pad: int
) -> Sequence[str]:
    """Get bng references that cover a multigeometry's bounding boxes."""
    return (list(set(
        [bng for geom in geometry.geoms
         for bng in _bng_geom_bounding_box(geom, resolution, pad)])))


def _on_edge(easting: Union[int, float],
             northing: Union[int, float],
             resolution: int) -> bool:
    """Test if point lies on edge."""
    return (True if (int(easting) % resolution == 0)
            or (int(northing) % resolution == 0) else False)


def _on_vertex(easting: Union[int, float],
               northing: Union[int, float],
               resolution: int) -> bool:
    """Test if point lies on vertex."""
    return (True if (int(easting) % resolution == 0)
            and (int(northing) % resolution == 0) else False)


def _bng_point(
    geometry: Point,
    resolution: int,
    pad: int
) -> Sequence[str]:
    """Get bng references for point geometries."""
    # Test for special cases
    if _on_edge(geometry.x, geometry.y, resolution):
        if _on_vertex(geometry.x, geometry.y, resolution):
            return [_coords_to_bng(geometry.x + adjust_x,
                                   geometry.y + adjust_y,
                                   resolution)
                    for adjust_x in [pad, -1 * pad]
                    for adjust_y in [pad, -1 * pad]]
        elif geometry.x % resolution == 0:
            # Vertical edge
            return [_coords_to_bng(geometry.x + adjust, geometry.y, resolution)
                    for adjust in [pad, -1 * pad]]
        else:
            # Horizontal edge
            return [_coords_to_bng(geometry.x, geometry.y + adjust, resolution)
                    for adjust in [pad, -1 * pad]]
    else:
        # Not on edge/vertex return single bng ref
        return [_coords_to_bng(geometry.x, geometry.y, resolution)]


def _bng_multipoint(
    geometry: MultiPoint,
    resolution: int,
    pad: int
) -> Sequence[str]:
    """Get bng references for multipoint geometries."""
    return (list(set(
        [bng for geom in geometry.geoms
         for bng in _bng_point(geom, resolution, pad)])))


def apply_bng_index(wkb: bytearray,
                    resolution: int,
                    how: str = None,
                    pad: int = 1) -> Sequence[str]:
    if resolution not in RESOLUTION:
        raise ValueError(f"resolution must be one of: {RESOLUTION}")

    geometry = loads(bytes(wkb))

    if isinstance(geometry, Point):
        return _bng_point(geometry, resolution, pad)
    elif isinstance(geometry, MultiPoint):
        return _bng_multipoint(geometry, resolution, pad)
    elif isinstance(geometry, Polygon):
        if (not how) or (how == 'bounding box'):
            return _bng_geom_bounding_box(geometry, resolution, pad)
    elif isinstance(geometry, MultiPolygon):
        if (not how) or (how == 'bounding box'):
            return _bng_multigeom_bounding_box(geometry, resolution, pad)
