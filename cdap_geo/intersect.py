from .typing import *
from .utils import spark, wkb, wkbs
from .indexing import calculate_bng_index
from pyspark.sql import functions as F, types as T


# Spatial Functions
@F.udf(returnType=T.DoubleType())
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

@F.udf(returnType=T.ArrayType(T.DoubleType()))
def bounds(data):
  return wkb(data).bounds


# GeoDataFrame Intersecting, returns GeoDataFrame not GeoSeries
def gpd_gdf_intersects(gdf: GeoDataFrame, other: BaseGeometry):
  return gdf[gdf.intersects(other)]

def gpd_gdf_intersection(gdf: GeoDataFrame, other: BaseGeometry):
  return gpd_gdf_intersects(gdf, other).clip(other)


# Intersecting UDF with MultiPolygon
def intersects_udf(left, right):
  @F.udf(returnType=T.BooleanType())
  def cond(left):
    return wkb(left).intersects(right)
  return cond(left)

def intersects_pudf(left: Series, right: Geometry) -> Series:
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
def index_intersects_udf(left, right):
  return wkb(left).intersects(wkb(right))

@F.pandas_udf(returnType=T.BooleanType())
def index_intersects_pudf(left, right):
  return wkbs(left).intersects(wkbs(right))

@F.udf(returnType=T.BinaryType())
def index_intersection_udf(left, right):
  return wkb(left).intersection(wkb(right)).wkb

@F.pandas_udf(returnType=T.BinaryType())
def index_intersection_pudf(left, right):
  return wkbs(left).intersection(wkbs(right)).wkb


# Spatial Index Join
def index_apply(column, resolution):
  '''Spatial Indexing
  currying resolution ∈ (1, 10, 100, 1_000, 10_000, 100_000)
  '''
  @F.udf(returnType=T.ArrayType(T.StringType()))
  def _index_apply(column):
    return calculate_bng_index(column, resolution=resolution, how='intersects')
  return _index_apply(column)

def index_sjoin(left, right, resolution):
  left = ( left
    .withColumn('index_left', F.monotonically_increasing_id()) )
  right = ( right
    .withColumnRenamed('geometry', 'geometry_right')
    .withColumn('index_right', F.monotonically_increasing_id()) )
  left_index = ( left
    .withColumn('index_spatial', index_apply('geometry', resolution))
    .withColumn('index_spatial', F.explode('index_spatial'))
    .select('index_left', 'index_spatial') )
  right_index = ( right
    .withColumn('index_spatial', index_apply('geometry_right', resolution))
    .withColumn('index_spatial', F.explode('index_spatial'))
    .select('index_right', 'index_spatial') )
  sdf = ( SparkDataFrame
    .join(left_index, right_index, on='index_spatial')
    .drop('index_spatial').distinct()
    .join(left, on='index_left')
    .join(right, on='index_right')
    .drop('index_left', 'index_right') )
  return sdf

def index_intersects(left, right, resolution):
  sdf = index_sjoin(left, right, resolution) \
    .filter(index_intersects_pudf('geometry', 'geometry_right'))
  return sdf

def index_intersection(left, right, resolution):
  sdf = index_sjoin(left, right, resolution) \
    .withColumn('geometry', index_intersection_pudf('geometry', 'geometry_right'))
  return sdf


# Bounding Box Join
def bbox_bounds(df, suffix'):
  return df \
    .withColumn('bounds', bounds('geometry'+suffix)) \
    .withColumn('minx'+suffix, F.col('bounds')[0]) \
    .withColumn('miny'+suffix, F.col('bounds')[1]) \
    .withColumn('maxx'+suffix, F.col('bounds')[2]) \
    .withColumn('maxy'+suffix, F.col('bounds')[3]) \
    .select('index'+suffix, 'minx'+suffix, 'miny'+suffix, 'maxx'+suffix, 'maxy'+suffix)

def bbox_join(left, right, lsuffix='', rsuffix='_right'):
  left = ( left
    .withColumnRenamed('geometry', 'geometry'+lsuffix)
    .withColumn('index', F.monotonically_increasing_id()) )
  right = ( right
    .withColumnRenamed('geometry', 'geometry'+rsuffix)
    .withColumn('index_right', F.monotonically_increasing_id()) )  
  l = bbox_bounds(left, lsuffix)
  r = bbox_bounds(right, rsuffix)
  lookup = l.join(r, on=None).filter(
    ~ ( (F.col('minx'+lsuffix) > F.col('maxx'+rlsuffix))  # east of
    | (F.col('miny'+lsuffix) > F.col('maxy'+rlsuffix))  # north of
    | (F.col('maxx'+lsuffix) < F.col('minx'+rlsuffix))  # west of
    | (F.col('maxy'+lsuffix) < F.col('miny'+rlsuffix)) )  # south of
  ).drop(
    'minx'+lsuffix, 'miny'+lsuffix, 'maxx'+lsuffix, 'maxy'+lsuffix,
    'minx'+rsuffix, 'miny'+rsuffix, 'maxx'+rsuffix, 'maxy'+rsuffix
  )
  df = lookup \
    .join(left, on='index') \
    .join(right, on='index_right') \
    .drop('index', 'index_right')
  return df

def bbox_intersects(left, right):
  sdf = bbox_join(left, right) \
    .filter(intersects_pudf('geometry', 'geometry_right'))
  return sdf

def bbox_intersection(left, right):
  sdf = bbox_join(left, right) \
    .withColumn('geometry', index_intersection_pudf('geometry', 'geometry_right'))
  return sdf
