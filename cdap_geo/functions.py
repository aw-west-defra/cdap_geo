from .typing import *
from .utils import spark, wkb, wkbs
import numpy as np
from pyspark.sql import functions as F, types as T
from pyproj import Transformer, CRS
from shapely.ops import transform
from shapely.geometry import Point
from geopandas._compat import import_optional_dependency


# Spatial Functions
@F.udf(returnType=T.FloatType())
def area(geom):
  return wkb(geom).area

@F.udf(returnType=T.BinaryType())
def unary_union(geoms):
  return sum(wkb(geom) for geom in geoms).wkb

def buffer(column, resolution, **kwargs):
  @F.udf(returnType=T.BinaryType())
  def _buffer(data):
    return wkb(data).buffer(resolution, **kwargs).wkb
  return _buffer(column)

@F.udf(returnType=T.ArrayType(T.FloatType()))
def bounds(data):
  return wkb(data).bounds

def crs(column, crs_from, crs_to=27700):
  project = Transformer.from_crs(CRS(f'EPSG:{crs_from}'), CRS(f'EPSG:{crs_to}'), always_xy=True).transform
  @F.udf(returnType=T.BinaryType())
  def _crs(data):
    return transform(project, wkb(data)).wkb
  return _crs(column)

@F.udf(returnType=T.FloatType())
def intersection_area(left, right):
  return wkb(left).intersection(wkb(right)).area


# GeoDataFrame Intersecting, returns GeoDataFrame not GeoSeries
def gdf_intersects(gdf: GeoDataFrame, other: BaseGeometry):
  return gdf[gdf.intersects(other)]

def gdf_intersection(gdf: GeoDataFrame, other: BaseGeometry):
  return gpd_gdf_intersects(gdf, other).clip(other)

# GeoPandas function
def gpd_drop_z(ds):
  ''' Drop Z coordinates from GeoSeries, returns GeoSeries
  Requires pygeos to be installed, and such I've added `import pygeos` to check.
  '''
  import_optional_dependency('pygeos')
  return gpd.GeoSeries.from_wkb(ds.to_wkb(output_dimension=2))

# Intersecting UDF with MultiPolygon
def gdf_intersects_udf(left, right: Geometry):
  @F.udf(returnType=T.BooleanType())
  def cond(left):
    return wkb(left).intersects(right)
  return cond(left)

def gdf_intersects_pudf(left: Series, right: Geometry) -> Series:
  @F.pandas_udf(returnType=T.BooleanType())
  def cond(left: Series) -> Series:
    return wkbs(left).intersects(right)
  return cond(left)


# Geometry UDFs
@F.udf(returnType=T.BooleanType())
def intersects_udf(left, right):
  return wkb(left).intersects(wkb(right))

@F.pandas_udf(returnType=T.BooleanType())
def intersects_pudf(left, right):
  return wkbs(left).intersects(wkbs(right))

@F.udf(returnType=T.BinaryType())
def intersection_udf(left, right):
  return wkb(left).intersection(wkb(right)).wkb

@F.pandas_udf(returnType=T.BinaryType())
def intersection_pudf(left, right):
  return wkbs(left).intersection(wkbs(right)).wkb


# Raster
def udf_pointify(col, resolution:int, as_struct:bool):
  '''Pointifying is like rasterisation but keeping the shapely geometry'''

  if as_struct:
    pointifyType = T.StructType([
      T.StructField('count', T.IntegerType(), True),
      T.StructField('mean', T.FloatType(), True),
      T.StructField('points', T.ArrayType(T.BinaryType()), True),
    ])
  else:
    pointifyType = T.ArrayType(T.BinaryType())
  
  @F.udf(returnType=pointifyType)
  def _pointify(col):
    geometry = wkb(col)
    xmin, ymin, xmax, ymax = geometry.bounds
    x, y = np.meshgrid(
      np.arange(xmin+resolution/2, xmax, resolution),
      np.arange(ymin+resolution/2, ymax, resolution),
    )
    
    points = GeoSeries(map(Point, zip(x.flat, y.flat)))
    isin = points.within(geometry)
    points = [p.wkb for p in points[isin]]
    
    if as_struct:
      return {
        'count': isin.sum().item(),  # item = to native (int)
        'mean': isin.mean().item(),  # item = to native (float)
        'points': points,
      }
    else:
      return points
    
  return _pointify(col)
