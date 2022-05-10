from .typing import *
from .utils import spark, wkb, wkbs
from pyspark.sql import functions as F, types as T


# Spatial Functions
@F.udf(returnType=T.FloatType())
def area(geom):
  return wkb(geom).area

@F.udf(returnType=T.BinaryType())
def unary_union(data):
  return sum(wkb(data) for geom in geoms).wkb

def buffer(column, resolution):
  @F.udf(returnType=T.BinaryType())
  def _buffer(data):
    return wkb(data).buffer(resolution).wkb
  return _buffer(column)

@F.udf(returnType=T.ArrayType(T.FloatType()))
def bounds(data):
  return wkb(data).bounds


# GeoDataFrame Intersecting, returns GeoDataFrame not GeoSeries
def gdf_intersects(gdf: GeoDataFrame, other: BaseGeometry):
  return gdf[gdf.intersects(other)]

def gdf_intersection(gdf: GeoDataFrame, other: BaseGeometry):
  return gpd_gdf_intersects(gdf, other).clip(other)


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


# Sedona Intersecting
def sedona_intersects(df0, df1):
  df0.createOrReplaceTempView('df0')
  df1.createOrReplaceTempView('df1')
  return spark.sql('SELECT df0.* FROM df0, df1 WHERE ST_Intersects(df0.geometry, df1.geometry)')

def sedona_intersection(df0, df1):
  df0.createOrReplaceTempView('df0')
  df1.createOrReplaceTempView('df1')
  df2 = spark.sql('SELECT df0.*, ST_Intersection(df0.geometry, df1.geometry) as geometry_2 FROM df0, df1')
  # SQL doesn't overwrite columns just has 2 with the same name
  return df2.drop('geometry').withColumnRenamed('geometry_2', 'geometry')


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
