from cdap_geo_utils.utils import spark, wkb
from cdap_geo_utils.typing import *
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

def index_join(left: SparkDataFrame, right: SparkDataFrame,
    keep_right: bool, keep_indexes: bool, resolution: int):
  right = right.withColumnRenamed('geometry', 'geometry_right')
  cols = set(left.columns)
  cols.update(['geometry_right'])
  if keep_indexes:
    cols.update(['geometry_indexes'])
  if keep_right:
    cols.update(right.columns)
  left = left.withColumn('id_left', F.monotonically_increasing_id())
  right = right.withColumn('id_right', F.monotonically_increasing_id())
  left = left.withColumn('geometry_indexes', apply_index('geometry', resolution))
  right = right.withColumn('geometry_index', apply_index('geometry_right', resolution))
  left = left.withColumn('geometry_index', F.explode('geometry_indexes'))
  right = right.withColumn('geometry_index', F.explode('geometry_index'))
  sdf = left.join(right, on='geometry_index', how='inner')
  sdf = sdf.groupBy('id_left', 'id_right').agg(*[index_collect(col, False) for col in cols])
  sdf = sdf.drop('id_right')
  return sdf

def index_distinct(sdf: SparkDataFrame,
    keep_right: bool, union: bool):
  if not keep_right:
    cols = sdf.columns
    cols.remove('geometry_right')
    sdf = sdf.groupBy('id_left').agg(*[index_collect(col, union) for col in cols])
  sdf = sdf.drop('id_left')
  return sdf

def index_intersects(left: SparkDataFrame, right: SparkDataFrame,
    keep_right: bool = False, keep_indexes: bool = False, resolution: int = 100):
  sdf = index_join(left, right, keep_right, keep_indexes, resolution)
  sdf = sdf.filter(index_intersects_udf('geometry', 'geometry_right'))
  sdf = index_distinct(sdf, keep_right, union=False)
  return sdf

def index_intersection(left: SparkDataFrame, right: SparkDataFrame,
    keep_right: bool = False, keep_indexes: bool = False, resolution: int = 100):
  sdf = index_join(left, right, keep_right, keep_indexes, resolution)
  sdf = sdf.withColumn('geometry', index_intersection_udf('geometry', 'geometry_right'))
  sdf = index_distinct(sdf, keep_right, union=False)
  return sdf
