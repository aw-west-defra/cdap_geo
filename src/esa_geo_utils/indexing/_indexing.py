from itertools import product
from typing import Iterator, Sequence, Tuple, Union
from warnings import warn

from shapely.geometry import (
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
    box,
)
from shapely.prepared import PreparedGeometry, prep
from shapely.wkb import loads

# CONSTANTS
# Note no I
LETTERS = "ABCDEFGHJKLMNOPQRSTUVWXYZ"
NUMBERS_LENGTH = {1: 5, 10: 4, 100: 3, 1_000: 2, 10_000: 1, 100_000: 0}
RESOLUTION = (1, 10, 100, 1_000, 10_000, 100_000)
RESOLUTION_LENGTH = {2: 100000, 4: 10000, 6: 1000, 8: 100, 10: 10, 12: 1}
# First letter identifies the 500x500 km grid
G500 = {
    "S": (0, 0),
    "T": (500000, 0),
    "N": (0, 500000),
    "O": (500000, 500000),
    "H": (0, 1000000),
    "J": (500000, 1000000),
}
# Second letter identifies the 100x100 km grid
G100 = {
    "A": (0, 400000),
    "B": (100000, 400000),
    "C": (200000, 400000),
    "D": (300000, 400000),
    "E": (400000, 400000),
    "F": (0, 300000),
    "G": (100000, 300000),
    "H": (200000, 300000),
    "J": (300000, 300000),
    "K": (400000, 300000),
    "L": (0, 200000),
    "M": (100000, 200000),
    "N": (200000, 200000),
    "O": (300000, 200000),
    "P": (400000, 200000),
    "Q": (0, 100000),
    "R": (100000, 100000),
    "S": (200000, 100000),
    "T": (300000, 100000),
    "U": (400000, 100000),
    "V": (0, 0),
    "W": (100000, 0),
    "X": (200000, 0),
    "Y": (300000, 0),
    "Z": (400000, 0),
}
# Helpers for making box from grid ref
Y_START = {10000: 3, 1000: 4, 100: 5, 10: 6, 1: 7}
Y_STOP = {10000: 4, 1000: 6, 100: 8, 10: 10, 1: 12}


def _coords_to_bng(
    eastings: Union[int, float], northings: Union[int, float], resolution: int
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
    first_letter_index = (
        (19 - northings_quotient)
        - (19 - northings_quotient) % 5
        + (eastings_quotient + 10) // 5
    )
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
    return_grid_coords: bool = False,
) -> Union[Sequence[str], Iterator[Tuple[str, Sequence[Union[int, float]]]]]:
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

    # Check if upper bounds fall within the same grid as upper
    if x2 - x1 <= resolution:
        if y2 - y1 <= resolution:
            if not return_grid_coords:
                # Shortcut for bounding box calculations
                return [_coords_to_bng(x1, y1, resolution)]

    # Use range to calculate the axes
    eastings_axis = range(x1, x2, resolution)
    northings_axis = range(y1, y2, resolution)

    # Use product to generate the collection of coordinates
    grid_coordinates = list(product(eastings_axis, northings_axis))

    # Return bng strings
    bng = [
        _coords_to_bng(eastings, northings, resolution)
        for eastings, northings in grid_coordinates
    ]

    if return_grid_coords:
        return zip(bng, grid_coordinates)
    else:
        return bng


def _bng_multigeom_bounding_box(
    geometry: Union[MultiLineString, MultiPolygon], resolution: int, pad: int
) -> Sequence[str]:
    """Get bng references that cover a multigeometry's bounding boxes."""
    return list(
        set(
            [
                bng
                for geom in geometry.geoms
                for bng in _bng_geom_bounding_box(geom, resolution, pad)
            ]
        )
    )


def _on_edge(
    easting: Union[int, float], northing: Union[int, float], resolution: int
) -> bool:
    """Test if point lies on edge."""
    return (
        True
        if (int(easting) % resolution == 0) or (int(northing) % resolution == 0)
        else False
    )


def _on_vertex(
    easting: Union[int, float], northing: Union[int, float], resolution: int
) -> bool:
    """Test if point lies on vertex."""
    return (
        True
        if (int(easting) % resolution == 0) and (int(northing) % resolution == 0)
        else False
    )


def _bng_point(geometry: Point, resolution: int, pad: int) -> Sequence[str]:
    """Get bng references for point geometries."""
    # Test for special cases
    if _on_edge(geometry.x, geometry.y, resolution):
        if _on_vertex(geometry.x, geometry.y, resolution):
            return [
                _coords_to_bng(geometry.x + adjust_x, geometry.y + adjust_y, resolution)
                for adjust_x in [pad, -1 * pad]
                for adjust_y in [pad, -1 * pad]
            ]
        elif int(geometry.x) % resolution == 0:
            # Vertical edge
            return [
                _coords_to_bng(geometry.x + adjust, geometry.y, resolution)
                for adjust in [pad, -1 * pad]
            ]
        else:
            # Horizontal edge
            return [
                _coords_to_bng(geometry.x, geometry.y + adjust, resolution)
                for adjust in [pad, -1 * pad]
            ]
    else:
        # Not on edge/vertex return single bng ref
        return [_coords_to_bng(geometry.x, geometry.y, resolution)]


def _bng_multipoint(geometry: MultiPoint, resolution: int, pad: int) -> Sequence[str]:
    """Get bng references for multipoint geometries."""
    return list(
        set(
            [
                bng
                for geom in geometry.geoms
                for bng in _bng_point(geom, resolution, pad)
            ]
        )
    )


def _bng_geom_index(
    geometry: Union[LineString, Polygon],
    resolution: int,
    pad: int,
    return_boxes: bool = False,
    prepared_geometry: PreparedGeometry = None,
) -> Union[Sequence[str], Sequence[Tuple[str, box]]]:
    """Return BNG refs for intersecting boxes."""
    # Get coords
    coords = list(_bng_geom_bounding_box(geometry, resolution, pad, True))
    # Shortcut for single box (must intersect geometry)
    if len(coords) == 1 and not return_boxes:
        return [coords[0][0]]

    # Make grid boxes
    boxes = [
        box(eastings, northings, eastings + resolution, northings + resolution)
        for _, (eastings, northings) in coords
    ]

    if not prepared_geometry:
        # Prepare geometry for fast intersections
        prepared_geometry = prep(geometry)

    if return_boxes:
        # Return BNG id and box
        return [
            (coords[idx][0], box)
            for idx, box in enumerate(boxes)
            if prepared_geometry.intersects(box)
        ]

    else:
        # Return bng ids of intersecting boxes
        return [
            coords[idx][0]
            for idx, box in enumerate(boxes)
            if prepared_geometry.intersects(box)
        ]


def _bng_multigeom_index(
    geometry: Union[MultiLineString, MultiPolygon],
    resolution: int,
    pad: int,
) -> Sequence[str]:
    """Return BNG refs for intersecting boxes for multigeoms."""
    return list(
        set(
            [
                bng
                for geom in geometry.geoms
                for bng in _bng_geom_index(geom, resolution, pad)
            ]
        )
    )


def _bng_geom_marked(
    geometry: Union[LineString, Polygon],
    resolution: int,
    pad: int,
) -> Sequence[Tuple[str, bool]]:
    """Return BNG refs and boolean showing whether box is fully inside the geometry."""
    # Prepare geometry for fast intersections
    prepared_geometry = prep(geometry)
    # Get boxes
    boxes = list(_bng_geom_index(geometry, resolution, pad, True, prepared_geometry))
    # Return bng ids of intersecting boxes
    return [
        (bng, True) if prepared_geometry.contains_properly(box) else (bng, False)
        for bng, box in boxes
    ]


def _bng_multigeom_marked(
    geometry: Union[MultiLineString, MultiPolygon],
    resolution: int,
    pad: int,
) -> Sequence[str]:
    """Return BNG refs and boolean for intersecting boxes for multigeoms."""
    return list(
        set(
            [
                (bng, inside)
                for geom in geometry.geoms
                for bng, inside in _bng_geom_marked(geom, resolution, pad)
            ]
        )
    )


# Collect functions into dictionary
METHODOLOGY = {
    "Point": {"intersects": _bng_point},
    "MultiPoint": {"intersects": _bng_multipoint},
    "Linestring": {
        "bounding box": _bng_geom_bounding_box,
        "intersects": _bng_geom_index,
    },
    "MultiLinestring": {
        "bounding box": _bng_multigeom_bounding_box,
        "intersects": _bng_multigeom_index,
    },
    "Polygon": {
        "bounding box": _bng_geom_bounding_box,
        "intersects": _bng_geom_index,
        "contains": _bng_geom_marked,
    },
    "MultiPolygon": {
        "bounding box": _bng_multigeom_bounding_box,
        "intersects": _bng_multigeom_index,
        "contains": _bng_multigeom_marked,
    },
}


def calculate_bng_index(
    wkb: bytearray, resolution: int, how: str = None, pad: int = 1
) -> Sequence[str]:
    """Calculate a British National Grid index at a given resolution.

    For a well-known binary representation of a point, linestring, polygon,
    multipoint, multilinestring or multipolygon the British National Grid (BNG)
    reference(s) is provided for that geometry at a given resolution.
    Possible resolutions are 1m, 10m, 100m, 1km, 10km, 100km.

    For points/multipoints a single 'how' method of 'intersects' is available, this
    references the BNG reference of the grid cell the point falls in, the 2 grid
    references straddled if the point lies on the line dividing two grid cells, or
    the 4 grid references straddled if the point lies on a corner vertex shared by
    four grid cells.

    For linestrings/multilinestrings 'how' methods of 'bounding box' and 'intersects'
    are available. The 'bounding box' method returns all grid cells covering the
    bounding box of the geometry; the 'intersects' method is preferred by default
    as this returns only the subset of grid cells that the line intersects.

    For polygons/multipolygons 'how' methods of 'bounding box', 'intersects' and
    'contains' are available. The 'bounding box' method returns all grid cells
    covering the bounding box of the geometry; the 'intersects' method is the
    default returning the grid cells that the polygon intersects; the 'contains'
    methods additionally returns a boolean for each grid cell that the polygon
    intersects with indicating whether that grid cell is contained by the polygon
    (True) or intersects but is not contained (False).

    The function can be applied over pandas/geopandas dataframe columns where that
    column contains a wellknown binary (wkb) representation of a geometry. However,
    it is primarily intended to be wrapped in a pyspark user defined function (udf)
    and applied to a spark dataframe column which has wellknown binary format
    geometry data.

    Example:
        >>> # Create UDF
            from esa_geo_utils.indexing import calculate_bng_index
            from pyspark.sql.functions import udf
            from pyspark.sql.types import StringType, ArrayType
            from typing import Sequence

            @udf(returnType=ArrayType(StringType()))
            def apply_index(wkb: bytearray) -> Sequence[str]:
                # Single argument function for udf
                return calculate_bng_index(wkb, resolution=100, how = 'intersects')

            # Apply to spark dataframe
            df = df.withColumn("bng_index", apply_index('geometry'))

    Args:
        wkb (bytearray): Well-known binary representation of a geometry. # noqa DAR003
        resolution (int): Resolution of British National Grid cell(s) to return.
        Available resolutions are 1, 10, 100, 1000, 10000, 100000 (metres).
        how (str): Indexing method of: bounding box, intersects (default), contains.
        pad (int): As grid references must be integers, pad is a tolerance
        that defaults to 1 and is unlikely to require adjustment.

    Returns:
        list[str] or list[tuple[str, bool]]: List of British National Grid References
        or List of Tuples of BNG references and boolean 'contains' relationships.

    Raises:
        ValueError: If supported method for 'how' not given, or given method not
        available for geometry type.

    """
    if resolution not in RESOLUTION:
        raise ValueError(f"'resolution' must be one of: {RESOLUTION}")

    if not how:
        # Set default for how to intersects
        how = "intersects"
        warn(
            "'how' not set, defaulting to 'intersects'",
        )

    if how not in ["bounding box", "intersects", "contains"]:
        raise ValueError(
            "'how' must be one of 'bounding box', 'intersects' or 'contains'"
        )

    # Load geometry from wkb
    geometry = loads(bytes(wkb))

    # Get appropriate function
    method = (
        METHODOLOGY.get(geometry.geom_type).get(how)
        if METHODOLOGY.get(geometry.geom_type)
        else None
    )

    if not method:
        raise ValueError(
            f"how = '{how}' not defined for {geometry.geom_type} geometry type."
        )

    return method(geometry, resolution, pad)


def wkt_from_bng(bng_reference: str) -> str:
    """Get WKT representation of bng reference.

    Example:
        >>> bng = wkt_from_bng("TQ3070")

    Args:
        bng_reference (str): British National Grid reference.

    Returns:
        str: Well-known text representation of British National Grid reference box.

    Raises:
        ValueError: If `bng_reference` is incorrect length to derive resolution.
    """
    # Get resolution from bng reference
    resolution = RESOLUTION_LENGTH.get(len(bng_reference))
    if not resolution:
        raise ValueError("Incorrect length bng_reference provided.")
    # Get letter coordinates
    grid_500 = G500.get(bng_reference[0])
    grid_100 = G100.get(bng_reference[1])
    # Calculate eastings and northings for lower left
    ll_x = grid_500[0] + grid_100[0]
    ll_y = grid_500[1] + grid_100[1]
    if resolution < 100000:
        y_index_start = Y_START.get(resolution)
        y_index_stop = Y_STOP.get(resolution)
        out_res = NUMBERS_LENGTH.get(resolution)
        ll_x = ll_x + (int(bng_reference[2:y_index_start][:out_res]) * resolution)
        ll_y = ll_y + (
            int(bng_reference[y_index_start:y_index_stop][:out_res]) * resolution
        )

    return f"""POLYGON(({ll_x} {ll_y},
                        {ll_x + resolution} {ll_y},
                        {ll_x + resolution} {ll_y + resolution},
                        {ll_x} {ll_y + resolution},
                        {ll_x} {ll_y}))"""
