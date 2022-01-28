"""Create grid-based spatial indexes.

Basic Usage
===========

Calculate the grid index or indices for a geometry provided in
well-known binary format at a given resolution:

Example:
    >>> from shapely.geometry import Point
    >>> pnt = Point(555000, 185000)

    >>> bng_pnt = calculate_bng_index(
        wkb = pnt.wkb,
        resolution = 100,
    )

Changing Resolution
===================

Indices can be calculated for cell sizes of 1m, 10m, 100m, 1000m, 10000m and 100000m:

Example:
    >>> from shapely.geometry import LineString
    >>> line = LineString([(450750, 175000), (535000, 195250)])

    >>> bng_line = calculate_bng_index(
        wkb = line.wkb,
        resolution = 1000,
    )

Index Creation Options
======================

The ``how`` argument can be used to change the kind of indices created.

Points and Multi-Points
-----------------------

The default and only option for ``how`` is 'intersects'. This returns the
British National Grid index that the point falls within. If the point lies
on an edge or corner of the grid cell then 2 or 4 grid cells indices are
returned as appropriate.

LineStrings and MultiLineStrings
--------------------------------

The default options for ``how`` is 'intersects'. This returns all indices for
the British National Grid cells that the line geometry intersects. An alternative
option is 'bounding box', which returns all indices that intersect with the
bounding box of the line geometry:

Example:
    >>> bng_line = calculate_bng_index(
        wkb = line.wkb,
        resolution = 100,
        how = 'bounding box'
    )

Although bounding boxes are fast to compute, in most cases 'intersects' will be
preferable as bounding box indexing, particularly at higher resolutions, will lead
to considerable redundancy.

Polygons and MultiPolygons
--------------------------

The default option for ``how`` is 'intersects', but alternative options of
'bounding box' and 'contains' are also available. The 'bounding box' returns
the British National Grid indices which intersect the Polygon bounding box.
The 'contains' option returns one or more tuples containing the indices that
intersect the Polygon and a boolean, where ``true`` indicates that the grid
cell is contained within the Polygon and ``false`` that the grid cell intersects
the Polygon, but doesn't lie within it (e.g. the cell crosses the Polygon boundary).

Example:
    >>> from shapely.geometry import Polygon
    >>> poly = Polygon([(535000, 175000),
                        (555250, 185000),
                        (556000, 162500),
                        (527500, 160333),
                        (535000, 175000)])

    >>> bng_poly = calculate_bng_index(
        wkb = poly.wkb,
        resolution = 100,
        how = 'contains'
    )

Intended Usage
==============

The top-level ``calculate_bng_index()`` function is intended to be applied
over a column of geometries. The approach will support mixes of geometry types
in a single column. Although it is primarily intended for use in Spark, we
first present an example using ``geopandas`` which may be more familiar:

Example:
    >>> import geopandas
    >>> gdf = geopandas.read_file('some file of interest')

    >>> bng = gdf.apply(lambda row: calculate_bng_index(row.geometry.wkb, 100),
                        index = 1)

When using the function in spark, the same approach applies, however you first
need to create a user-defined function (udf).

    >>> from pyspark.sql.functions import udf
    >>> from pyspark.sql.types import StringType, ArrayType
    >>> from typing import Sequence

    >>> @udf(returnType=ArrayType(StringType()))
    >>> def apply_index(wkb: bytearray) -> Sequence[str]:
            return calculate_bng_index(wkb, resolution=100, how='intersects')

This user defined function can then be applied to a spark dataframe, assuming it stores
the geometry in well-known binary format:

Example:
    >>> sdf = spark.read.parquet('some parquet file of interest')
    >>> sdf = sdf.withColumn('bng', apply_index('geometry'))

The intent of the indexing is that it can then be used to benefit geospatial
filtering and joining operations.

Get British National Grid Cell Geometries
=========================================

A top-level helper function is provided for simple translation of British National
Grid references into well-known text that can be plotted. The resolution is inferred
from each reference:

Example:
    >>> import geopandas
    >>> from shapely.wkt import loads

    >>> box = wkt_from_bng("TQ3415")
    >>> gdf = geopandas.GeoDataFrame(geometry = [box])
    >>> gdf.plot()

The ``wkt_from_bng()`` function is also designed to be applied to collections of
references:

Example:
    >>> import geopandas
    >>> from shapely.wkt import loads

    >>> boxes = list(map(wkt_from_bng, ["TQ3415", "SP4087", "SS9015"]))
    >>> gdf = geopandas.GeoDataFrame(geometry = boxes)
    >>> gdf.plot()

"""

from cdap_geo_utils.indexing._indexing import calculate_bng_index, wkt_from_bng
