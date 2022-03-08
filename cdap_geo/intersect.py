from .typing import *
from .utils import spark, wkb
from esa_geo_utils.indexing import calculate_bng_index
from pyspark.sql import functions as F, types as T


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
    return GeoSeries.from_wkb(left).intersects(right)
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
def index_apply(column: bytearray, resolution: int):
  # currying resolution
  @F.udf(returnType=T.ArrayType(T.StringType()))
  def _index_apply(data: bytearray):
    return calculate_bng_index(data, resolution=resolution, how='intersects')
  return _index_apply(column)

@F.udf(returnType=T.BooleanType())
def index_intersects_udf(left, right):
  return wkb(left).intersects(wkb(right))

@F.udf(returnType=T.BinaryType())
def index_intersection_udf(left, right):
  return wkb(left).intersection(wkb(right)).wkb

@F.udf(returnType=T.BinaryType())
def index_unary_union(data):
  return sum(wkb(data) for geom in geoms).wkb

def buffer(column, resolution):
  @F.udf(returnType=T.BinaryType())
  def _buffer(data):
    return wkb(data).buffer(resolution).wkb
  return _buffer(column)


# Index
def index_collect(col: set, union: bool):
  if union and col == 'geometry':
    row = index_unary_union(F.collect_list(col))
  else:
    row = F.first(col)
  return row.alias(col)

def index_fun(left: SparkDataFrame, right: SparkDataFrame, fun: Callable,
    keep_right: bool = False, keep_indexes: bool = False, resolution: int = 100):
  # Drop
  if not keep_right:
    right = right.select('geometry')
  # Index
  right = right.withColumnRenamed('geometry', 'geometry_right')
  left = left.withColumn('id_left', F.monotonically_increasing_id())
  right = right.withColumn('id_right', F.monotonically_increasing_id())
  left = left.withColumn('geometry_indexes', index_apply('geometry', resolution))
  right = right.withColumn('geometry_index', index_apply('geometry_right', resolution))
  left_id = left.select('id_left', 'geometry_indexes')
  right_id = right.select('id_right', 'geometry_index')
  left_id = left_id.withColumn('geometry_index', F.explode('geometry_indexes'))
  right_id = right_id.withColumn('geometry_index', F.explode('geometry_index'))
  lookup = left_id.join(right_id, on='geometry_index', how='inner')
  lookup = lookup.select('id_left', 'id_right').distinct()
  # Join
  sdf = lookup.join(left, on='id_left', how='inner').join(right, on='id_right', how='inner')
  # Drop
  sdf = sdf.drop('id_right', 'geometry_index')
  if not keep_indexes:
    sdf = sdf.drop('geometry_indexes')
  # Function
  sdf = fun(sdf)
  # Drop
  if not keep_right:
    sdf = sdf.groupBy('id_left').agg(*[index_collect(col, False) for col in sdf.columns])
    sdf = sdf.drop('geometry_right')
  sdf = sdf.drop('id_left')
  return sdf

def index_intersects(left: SparkDataFrame, right: SparkDataFrame, **kwargs):
  fun = lambda sdf:  sdf.filter(index_intersects_udf('geometry', 'geometry_right'))
  return index_fun(left, right, fun, **kwargs)

def index_intersection(left: SparkDataFrame, right: SparkDataFrame, **kwargs):
  fun = lambda sdf:  sdf.withColumn('geometry', index_intersection_udf('geometry', 'geometry_right'))
  return index_fun(left, right, fun, **kwargs)
